# Operations Runbook: Agent-Native Spark Connect on EKS

This runbook covers standing up, operating, and tearing down the production
reference deployment: an OSS **Apache Spark Connect** server on **EKS**
(k8s-native), with a **Hive Metastore** catalog, **Iceberg** tables on **real
S3**, **mTLS-verified per-principal identity**, and **Spark executors as pods**.

It ties together the components built across PRs #3, #5, #6, #7, #8, #9. The
single-EC2 build (PRs #1, #2) is the optional cheap **dev tier** and is covered
briefly at the end.

> **Format:** Iceberg (not Delta). Delta cannot be a Spark AUTO CDC target, it
> lacks `SupportsRowLevelOperations`, so Iceberg is the correct and only viable
> native CDC sink. See the image README for the AUTO CDC version detail.

---

## 1. Architecture at a glance

```
agent sandbox / user laptop
  │  pyspark-client  →  local egress sidecar (holds per-principal client cert)   [deploy/spark-omnigent, PR #4]
  │  mTLS
  ▼
internal NLB (mTLS port 15009)                                                   [deploy/eks/connect, PR #9]
  ▼
┌─ Spark Connect pod (namespace: spark) ─────────────────────────────┐
│  Envoy sidecar  ── terminates mTLS, verifies client cert vs CA,    │  [adapted from deploy/auth, PR #3]
│                    sets x-connect-principal from cert SAN,         │
│                    injects PSK, forwards h2c → 127.0.0.1:15002     │
│  Spark Connect driver (loopback 15002 ONLY)                        │  [image: deploy/eks/images, PR #6]
│    PrincipalPinningInterceptor: REJECT if user_id ≠ principal      │
│    client-mode Spark → schedules executor PODS                     │
└────────────────────────────────────────────────────────────────────┘
        │ creates                         │ catalog              │ storage
        ▼                                 ▼                      ▼
  executor pods (executor node group)   HMS thrift:9083        S3 warehouse (Iceberg)
  [PR #5 node group]                    [deploy/eks/hms, PR #8] [PR #5 bucket, IRSA]
```

**The security guarantee (enforced, tested):** every request reaches Spark only
through Envoy mTLS; the client cert SAN is the principal; the interceptor pins
the Spark `user_id` to that principal and fails closed otherwise. There is **no
path to raw 15002** (loopback-bound, no Service targets it).

**By convention (not hard-enforced):** per-principal schema isolation
(`sandbox_<principal>`) and fleet-scoped catalog access. HMS/Iceberg do not
enforce per-user grants, that is deliberate and documented.

---

## 2. Prerequisites

- AWS profile **`ssa-deploy`** (IAM user `safe-spark-deploy`, account
  `${AWS_ACCOUNT_ID}`), region **`us-east-1`**. Never use root keys.
- Remote Terraform state: bucket **`${TFSTATE_BUCKET}`**, lock table
  **`ssa-tf-locks`** (already provisioned).
- Tools: `terraform >= 1.7`, `aws` v2, `kubectl`, `kustomize`, `helm` (optional),
  `docker`, and `openssl`/`easy-rsa` for certs.
- All live secrets (CAs, keys, PSK, tfvars, `.ovpn`, kubeconfig) live **outside
  the repo** in `~/ssa-deploy/` (perms `700`). Nothing secret is ever committed.

---

## 3. Deployment order

Deploy bottom-up; each layer depends on the previous one's outputs.

1. **EKS cluster + RDS + S3 + IRSA** (`deploy/eks/terraform`, PR #5)
   ```bash
   cd deploy/eks/terraform
   cp terraform.tfvars.example terraform.tfvars   # edit: profile ssa-deploy, region, CIDRs, users
   terraform init \
     -backend-config="bucket=${TFSTATE_BUCKET}" \
     -backend-config="key=eks/terraform.tfstate" \
     -backend-config="dynamodb_table=ssa-tf-locks" \
     -backend-config="region=us-east-1" \
     -backend-config="encrypt=true"
   terraform plan  -var-file=terraform.tfvars -out tfplan      # review resources + cost
   terraform apply tfplan                                       # creates billable infra
   aws eks update-kubeconfig --name <cluster> --profile ssa-deploy --region us-east-1
   ```
   Record the outputs: spark IRSA role ARN, hms IRSA role ARN, RDS endpoint +
   secret ARN, warehouse S3 bucket.

2. **Build + push the Spark image** (`deploy/eks/images/spark-connect`, PR #6)
   ```bash
   # Tier A (released: Spark 4.1.2 + Iceberg 1.11.0). build.sh reads env vars (INTERCEPTOR_JAR, IMAGE_TAG);
   # the registry is part of IMAGE_TAG, and --push (or PUSH=1) pushes.
   INTERCEPTOR_JAR=/path/to/principal-pinning-interceptor.jar \
   IMAGE_TAG=<ecr-repo>/ssa-spark/spark-connect:4.1.2-iceberg1.11.0 \
   ./build.sh --push
   ```
   Tier B (native AUTO CDC, Spark 5.0-SNAPSHOT + iceberg-port) is a `--tier-b`
   build off your existing `lakehouse/spark:5.0.0-snapshot-cdc` base.

3. **Hive Metastore** (`deploy/eks/hms`, PR #8)
   ```bash
   # Create the DB-password secret WITHOUT putting it on argv:
   umask 077; printf '%s' "$RDS_PASSWORD" > ~/ssa-deploy/hms-db-pass
   kubectl create namespace hive-metastore
   kubectl -n hive-metastore create secret generic hms-db \
     --from-literal=HMS_DB_URL="jdbc:postgresql://<rds-endpoint>:5432/metastore" \
     --from-literal=HMS_DB_USER="hive" \
     --from-file=HMS_DB_PASSWORD=~/ssa-deploy/hms-db-pass
   shred -u ~/ssa-deploy/hms-db-pass
   # Patch the SA role ARN + warehouse bucket in the overlay, then:
   kustomize build deploy/eks/hms/overlays/example | kubectl apply -f -
   ```
   The schema-init Job runs `schematool -initSchema` (idempotent) before the
   metastore serves on `hive-metastore.hive-metastore.svc.cluster.local:9083`.

4. **Connect server + executors** (`deploy/eks/connect`, PR #9), see §4 for the
   certs/PSK first, then:
   ```bash
   kustomize build deploy/eks/connect/overlays/example | kubectl apply -f -
   kubectl -n spark get pods,svc            # Connect pod Running; mTLS NLB provisioned
   ```

5. **Load real + messy data** (`deploy/data`, PR #7), see §6.

---

## 4. Secrets & certificates

Two **independent** mTLS layers (defense in depth): the Client-VPN CA (network,
EC2 tier) and the **Connect-layer CA** (application identity, used here). Issue
the Connect-layer material with the PR #3 tooling, into `~/ssa-deploy/`:

```bash
cd deploy/auth/certs
./make-ca.sh                     # Connect-layer root CA  → out/ (gitignored)
./issue-server-cert.sh connect   # server cert for the Envoy sidecar
# PSK: a single shared bearer the server requires; generated, never committed
openssl rand -hex 32 > ~/ssa-deploy/psk/connect.psk
```

Load them as k8s Secrets in the `spark` namespace (referenced by PR #9, never
committed):

The deployed secret **names and keys** are authoritative in `connect/base/secret.example.yaml`, create
those exact names (not the drifted ones):

```bash
# PSK (single-line base64url/hex): secret spark-connect-psk, key `token`
kubectl -n spark create secret generic spark-connect-psk --from-file=token=psk.token
# Envoy mTLS material from deploy/auth/certs/issue-all.sh: secret spark-connect-envoy-certs
kubectl -n spark create secret generic spark-connect-envoy-certs \
  --from-file=server.crt=out/server.crt \
  --from-file=server.key=out/server.key \
  --from-file=connect-ca.crt=out/connect-ca.crt
```

---

## 5. Onboarding a user or agent sandbox

One command issues a per-principal client cert whose **SAN == the principal ==
the Spark `user_id` == the schema owner**. A mismatch is impossible by
construction and rejected by the interceptor regardless.

```bash
cd deploy/auth/certs
./issue-client-cert.sh alice          # → out/alice.crt / out/alice.key (SAN: spiffe://safe-spark-agents/alice)
```

- **Human user:** hand them `alice.crt` + `alice.key` + `ca.crt`. They connect
  through a local mTLS proxy (or the sandbox egress sidecar) to
  `sc://<nlb-dns>:15009`.
- **Agent sandbox:** mount the cert **into the egress sidecar only**
  (`deploy/spark-omnigent`, PR #4), never into the agent container, and set
  `AGENT_PRINCIPAL=alice`. The sidecar verifies SAN == principal and refuses to
  start on mismatch.
- **Schema:** create the principal's namespace once:
  ```sql
  CREATE SCHEMA IF NOT EXISTS iceberg.sandbox_alice;
  ```

To **revoke**: remove/expire the client cert (and, for hard cutoff, rotate the
CA, §7). Drop the schema to remove data access.

---

## 6. Operating

**Load the messy dataset** (`deploy/data`, PR #7):
```bash
# Deterministic generate (chaos: nulls/malformed/dupes/late+out-of-order):
python -m generator generate --seed 42 --chaos-rate 0.08 --output ~/ssa-deploy/data
# Batch load → Iceberg bronze (via HMS, S3 warehouse, IRSA):
python load/load_to_iceberg.py --data-dir ~/ssa-deploy/data --catalog iceberg
# Streaming path → Kafka → Iceberg:
python load/stream_to_iceberg.py --chaos-rate 0.08 --seed 42
```

**Run pipelines:** the `safe-spark-agents/demos/pipelines/*` SDP pipelines run against
the Connect server by pointing `SPARK_REMOTE` at the mTLS endpoint. CDC: use
hand-rolled `MERGE` SCD1/SCD2 on the Tier-A (released 4.1) image, or native
`create_auto_cdc_flow(stored_as_scd_type=1)` on the Tier-B (5.0-SNAPSHOT) image.

**Scale executors:** dynamic allocation is on (min 0). To raise the ceiling,
bump the executor node group `max` in `deploy/eks/terraform` and/or
`spark.dynamicAllocation.maxExecutors` in the connect ConfigMap.

**Monitor:**
```bash
kubectl -n spark logs deploy/spark-connect -c spark-connect   # driver
kubectl -n spark logs deploy/spark-connect -c envoy           # mTLS proxy
kubectl -n spark get pods -l spark-role=executor              # executor pods
```

---

## 7. Rotation

- **PSK:** generate a new value, update the `connect-psk` Secret, `kubectl
  rollout restart deploy/spark-connect -n spark`, then update the Envoy config's
  injected bearer (same Secret), both sides come from the one Secret, so it's a
  single update + restart.
- **Client certs:** re-issue per principal (§5); short-lived certs are preferred.
- **CA rotation (hard revocation):** issue a new Connect-layer CA, update the
  `connect-mtls` `ca.crt`, restart the Connect pod, and re-issue all client
  certs. This invalidates every old cert at once.

---

## 8. Upgrading Spark (4.1 → 4.2 / AUTO CDC)

The whole point of self-managed Spark on EKS: **you own the version.**

1. Build a new image tag with the bumped `SPARK_VERSION` / `ICEBERG_VERSION`
   (and rebuild the interceptor jar against the new Spark API if needed).
2. Update the image tag in `deploy/eks/connect` (driver + executor use the same
   image) and `kubectl apply`.
3. For native AUTO CDC, switch to the **Tier-B** base (Spark 5.0-SNAPSHOT +
   `iceberg-port`) until a released Spark + released Iceberg 4.2 runtime exist,
   track both upstreams; that is the only thing standing between you and a fully
   released-bits stack.

---

## 9. Teardown

```bash
# App layers first:
kubectl delete -k deploy/eks/connect/overlays/example
kubectl delete -k deploy/eks/hms/overlays/example
# Then the cluster/infra (destroys billable resources):
cd deploy/eks/terraform && terraform destroy -var-file=terraform.tfvars
```
Delete leftover S3 warehouse objects and RDS snapshots manually if you want a
truly clean account. Revoke IAM access keys you no longer need.

---

## 10. Cost (rough, 24/7)

Dominated by: EKS control plane (~$73/mo) + executor/system nodes (on-demand,
scales to zero when idle if dynamic allocation drains) + RDS Postgres
(~$30–60/mo depending on size) + NLB (~$18/mo) + S3 (usage-based). Expect a
few hundred $/mo at rest; stop the node groups / scale to zero overnight to cut
compute. The single-EC2 dev tier (below) is far cheaper for iteration.

---

## 11. Dev tier (single EC2): optional

`deploy/aws` (PR #1) + `scripts/*connect-server*` (PR #2) stand up a single-EC2
Spark Connect server (one JVM, local execution) behind a Client VPN + NLB, with
the same Option-A auth available via the `enable_auth_proxy` path. Use it for
cheap iteration; it is **not** the distributed prod target. Same `ssa-deploy`
profile and remote-state pattern.

---

## Security model: what's enforced vs convention

| Property | Status |
|---|---|
| TLS in transit | **Enforced** (Envoy mTLS; Spark Connect has no native TLS) |
| Verified per-principal identity | **Enforced** (client cert SAN → `x-connect-principal`) |
| `user_id` pinned to principal (no spoofing) | **Enforced** (interceptor, fail-closed) |
| No raw-15002 bypass | **Enforced** (loopback bind, no Service target) |
| Per-agent schema isolation | **Convention** (`sandbox_<principal>`, not catalog-enforced) |
| Catalog/storage access | **Fleet-scoped** (HMS/Iceberg OSS has no per-user grants) |

This is the honest line: identity, encryption, and the no-bypass path are real
and tested; per-user *authorization* is convention, because OSS Spark + HMS +
Iceberg don't enforce it (and that's a documented, deliberate scope choice, not
a gap we papered over).
