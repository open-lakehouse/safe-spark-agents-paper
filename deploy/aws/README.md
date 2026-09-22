# Spark Connect 4.1 on AWS — Terraform IaC

Durable, remote, **OSS Apache Spark Connect 4.1.1** server on AWS for ~3 named human
users plus outbound connections from local agent sandboxes. No Databricks. Access is
private-only: an **AWS Client VPN** (mutual-TLS) is the front door, and an internal
**NLB** terminates TLS in front of the gRPC server. Admin shell is **SSM Session
Manager** — there is **no public SSH and no public 15002**.

> **NOT APPLIED — awaiting a personal AWS account.** Everything here has passed
> `terraform fmt`, `init -backend=false`, and `validate`. Nothing has been applied; no
> AWS resources exist yet.

---

## Architecture

```
                         AWS Client VPN  (mutual TLS, cert auth)
   3 users + admin  ───►  endpoint  ──────────────────────────────┐
   (.ovpn profiles)                                                │  client CIDR
                                                                   ▼  172.16.0.0/22
 ┌─────────────────────────────── VPC 10.42.0.0/16 ──────────────────────────────┐
 │                                                                                │
 │   public-a / public-b           ┌─ IGW ─ internet      ┌─ NAT GW (single) ─┐   │
 │   (NAT GW, IGW)                  │                      │  private egress    │   │
 │                                  │                      ▼                    │   │
 │   private-a  ◄── ASG (min=max=desired=1) ── EC2 r7i.xlarge, AL2023           │   │
 │     │            Launch Template          ├─ gp3 root (30GiB)                │   │
 │     │                                     └─ gp3 DATA vol (100GiB) ─► /srv/spark
 │     │                                          warehouse + metastore         │   │
 │     ▼                                          (survives instance replace)   │   │
 │   internal NLB ── TLS listener :15002 ──► target group TCP :15002 ──► EC2     │   │
 │     (ACM cert)                                                                │   │
 │                                                                              │   │
 │   DLM ─ daily snapshot of the data volume (retain 7)                         │   │
 │   SSM Session Manager ─ admin shell (no SSH)                                 │   │
 └────────────────────────────────────────────────────────────────────────────┘   │
                                                                                    │
   Clients connect over the VPN to:  sc://<nlb_dns_name>:15002;use_ssl=true ◄───────┘
```

**Components**

| Concern        | Resource |
|----------------|----------|
| Network        | Fresh VPC, 2 public + 2 private subnets across 2 AZs, IGW, **single** NAT GW |
| Access         | AWS Client VPN endpoint (certificate / mutual auth), authz rule for the VPC CIDR |
| Compute        | Launch Template + ASG (`min=max=desired=1`), Amazon Linux 2023, `r7i.xlarge` |
| Storage        | gp3 root + **dedicated** gp3 data volume at `/srv/spark` (warehouse + metastore) |
| Front door     | Internal NLB; TLS listener (ALPN h2) **or** TCP passthrough — see `enable_auth_proxy` |
| Admin          | SSM Session Manager via instance profile (`AmazonSSMManagedInstanceCore`) |
| Durability     | ASG self-heal + persistent data volume re-attach + **DLM daily snapshots** |

The server process (launcher + systemd unit) is authored by a **separate task** at
`connect/scripts/start-connect-server.sh`, `connect/scripts/stop-connect-server.sh`, and
`deploy/connect-server/spark-connect.service`. The EC2 user-data **clones this repo and
installs those paths** — it does not author them.

### Durability — how the data survives an instance replacement

The ASG keeps exactly one instance alive; if it fails health checks, reboots, or its
hardware degrades, the ASG **replaces** it from the Launch Template. The warehouse and
metastore must outlive that, so:

1. The **data volume is a standalone `aws_ebs_volume`** managed outside the
   LT/ASG lifecycle — it is *not* part of the launch template's block device mappings, so
   it is never recreated when the instance is replaced.
2. On boot, **user-data self-attaches** the volume by id (the instance profile grants
   `ec2:AttachVolume` scoped to `Project`-tagged volumes), resolves the NVMe device by
   matching the EBS volume id to the device serial, mounts it at `/srv/spark`, and only
   runs `mkfs` if the volume is blank (first ever boot).
3. EBS volumes are **AZ-bound**, so the ASG is **pinned to one private subnet/AZ**
   (`data_volume_az_index`) to guarantee a replacement lands where the volume lives. (The
   VPC still spans 2 AZs per the network contract; HA across AZs would need EFS or volume
   re-creation from snapshot, called out as a future option.)
4. A **DLM lifecycle policy** snapshots the data volume **daily** (retain 7, configurable)
   for point-in-time recovery and cross-AZ rebuilds.

### NLB listener modes — `enable_auth_proxy`

The NLB listener has two mutually exclusive modes, selected by `enable_auth_proxy`:

| | `enable_auth_proxy = false` (default) | `enable_auth_proxy = true` (Option A) |
|---|---|---|
| Listener protocol | **TLS** (terminates at NLB) | **TCP passthrough** (no termination) |
| Cert | ACM cert (`create_acm_cert` / `existing_acm_cert_arn`) | none at the NLB |
| ALPN | `HTTP2Preferred` (Spark Connect is gRPC/h2) | n/a — raw bytes forwarded |
| Target port | raw gRPC `connect_grpc_port` (15002) | `auth_proxy_port` (15009) |
| Client mTLS | not enforced here | **Envoy on-box** terminates TLS + client mTLS, speaks h2 to the server |

The TLS-termination mode is the default for a standalone server. **Option A** is the decided
design once the separate **on-box Envoy auth proxy** PR lands: the NLB must NOT terminate TLS,
because the client certificate has to reach Envoy for mutual-TLS authentication. Flipping
`enable_auth_proxy = true` switches the listener to plain TCP passthrough to `auth_proxy_port`
and retargets the target group + security-group rules accordingly — no other change here.

> TLS mode requires a **validated** ACM cert. Auto-validation only happens when
> `route53_zone_id` is set; otherwise use `create_acm_cert = false` with a pre-validated
> `existing_acm_cert_arn` (a precondition enforces this so `apply` can't hang on an
> unvalidated cert).

> **Option A forces all client traffic through Envoy mTLS.** When `enable_auth_proxy = true`,
> the direct `gRPC 15002 from Client VPN CIDR` ingress rule is **removed** — the server's only
> ingress is from the NLB security group on the Envoy `auth_proxy_port`. A VPN client cannot
> reach Spark Connect on 15002 directly, so it can't bypass Envoy's per-user mTLS identity
> enforcement (which would otherwise let a client spoof `user_id`). In non-proxy mode
> (`enable_auth_proxy = false`) the direct VPN→15002 rule is present, as before.

---

## Prerequisites

- Terraform `>= 1.6`, AWS CLI v2.
- A configured **named AWS profile** (the examples use `personal`).
- For the **Client VPN** (mutual auth): a server cert + a client root CA cert imported into
  **ACM**. Generate with [easy-rsa](https://github.com/OpenVPN/easy-rsa):
  ```bash
  ./easyrsa init-pki && ./easyrsa build-ca nopass
  ./easyrsa build-server-full server nopass
  ./easyrsa build-client-full client1.domain.tld nopass
  # import server cert (and the CA chain as the client-root) into ACM:
  aws acm import-certificate --profile personal --region us-east-1 \
    --certificate fileb://pki/issued/server.crt \
    --private-key fileb://pki/private/server.key \
    --certificate-chain fileb://pki/ca.crt
  ```
  Put the resulting ARNs in `vpn_server_cert_arn` and `vpn_client_root_cert_arn`.
- For the **NLB TLS cert**: either an existing ACM cert ARN (`existing_acm_cert_arn`), or
  set `create_acm_cert = true` with a `domain_name` (optionally a `route53_zone_id` to
  auto-validate).

---

## Init / plan / apply (against the `personal` profile)

The provider reads `aws_profile` from your tfvars, so the profile is wired in — no extra
`AWS_PROFILE` export needed. Commands, in order:

```bash
cd deploy/aws
cp terraform.tfvars.example terraform.tfvars   # then edit: cert ARNs, repo URL, users

terraform init
terraform plan  -var-file=terraform.tfvars -out tfplan
terraform apply tfplan
```

If you prefer to be explicit about the profile on the CLI:

```bash
AWS_PROFILE=personal terraform plan -var-file=terraform.tfvars
```

Key outputs after apply: `nlb_dns_name`, `client_vpn_endpoint_id`,
`client_vpn_config_download_command`, `data_volume_id`, `ssm_start_session_hint`.

### Connecting a client

Over the VPN, point the Spark Connect client at the NLB:

```
sc://<nlb_dns_name>:15002;use_ssl=true
```

(or `sc://<domain_name>:15002;use_ssl=true` if you created the Route53 alias).

---

## Onboard a Client VPN user

1. Issue a client cert for the user from the same CA:
   ```bash
   ./easyrsa build-client-full alice.spark nopass
   ```
2. Export the base client config (Terraform prints the exact command in
   `client_vpn_config_download_command`):
   ```bash
   aws ec2 export-client-vpn-client-configuration --profile personal --region us-east-1 \
     --client-vpn-endpoint-id <client_vpn_endpoint_id> --output text > spark-connect-client.ovpn
   ```
3. Append the user's client cert + key to the `.ovpn` (mutual auth):
   ```bash
   cat >> spark-connect-client.ovpn <<EOF
   <cert>
   $(cat pki/issued/alice.spark.crt)
   </cert>
   <key>
   $(cat pki/private/alice.spark.key)
   </key>
   EOF
   ```
4. Hand `spark-connect-client.ovpn` to the user; they import it into the AWS VPN Client (or
   any OpenVPN client).

> **`allowed_users` is onboarding inventory, not access enforcement.** With mutual-TLS,
> **any client cert signed by the CA authenticates** to the VPN — the list is who *should*
> have a profile (and a tag for audit), not a hard allowlist. Real per-user enforcement
> (revocation via CRL, and per-principal authz) is the job of the CA process + the on-box
> Envoy auth proxy (Option A). Revoke a departed user by rotating/CRL-ing their client cert.

> Admin shell (no SSH): resolve the instance id from the ASG, then
> `aws ssm start-session --profile personal --target <instance-id>`. See
> `ssm_start_session_hint`.

---

## Teardown

One command tears everything down (data volume included, since `prevent_destroy = false`):

```bash
cd deploy/aws
terraform destroy -var-file=terraform.tfvars
```

> The DLM **snapshots are retained** independently of the volume — delete them manually if
> you want a truly clean account. For production, flip the data volume's
> `prevent_destroy` to `true` so `destroy` can't nuke the warehouse by accident.

---

## State

Defaults to **local state** so the first run needs zero prerequisites. For shared/team use,
uncomment the S3 backend block in `versions.tf` (bucket + DynamoDB lock table), then
`terraform init -migrate-state`. See the `TODO` there.

---

## File map

| File | What |
|------|------|
| `versions.tf` | Terraform + provider version pins; S3 backend TODO |
| `providers.tf` | AWS provider, region/profile, default tags |
| `variables.tf` | All inputs (region, instance type, CIDRs, certs, users, …) |
| `network.tf` | VPC, subnets, IGW, NAT GW, routing, shared `locals` |
| `security_groups.tf` | Connect SG (15002 from VPN only) + NLB SG |
| `vpn.tf` | Client VPN endpoint, association, authz rule, logs |
| `iam.tf` | Instance role/profile: SSM + data-volume self-attach |
| `data_volume.tf` | Dedicated gp3 data volume + DLM daily snapshot policy |
| `compute.tf` | AL2023 AMI, Launch Template, single-instance ASG |
| `acm.tf` | NLB cert: create-via-DNS or bring-existing |
| `nlb.tf` | Internal NLB, TCP target group, TLS listener |
| `outputs.tf` | NLB DNS, VPN endpoint/association, SSM hint, etc. |
| `templates/user-data.sh.tftpl` | Boot: attach/mount volume, install JDK17+Spark, install the systemd unit |
| `terraform.tfvars.example` | Copy to `terraform.tfvars` and edit |
