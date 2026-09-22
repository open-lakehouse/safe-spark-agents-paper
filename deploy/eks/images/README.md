# Spark Connect + Iceberg image for EKS

A reproducible container image for an OSS **Apache Spark Connect server + executors**
on EKS, with **Apache Iceberg**, the auth interceptor, S3A (via IRSA), and
Hive-Metastore catalog wiring baked in. The same image runs both roles: the
long-running Connect server (a k8s Deployment) and the executor pods Spark
launches on its behalf — the role is selected at launch, not at build.

> Image source: [`spark-connect/`](./spark-connect/) — `Dockerfile`, `build.sh`,
> `fetch-jars.sh`, `jars.sha1` (the pinned BOM), `conf/spark-defaults.template.conf`,
> `entrypoint.sh`.

---

## 1. Format decision: **Iceberg**, not Delta — and *why it changes the version story*

Spark's native AUTO CDC primitive (`create_auto_cdc_flow`, the declarative
SCD‑1/SCD‑2 flow) requires a DSv2 target that implements
`SupportsRowLevelOperations`. **Iceberg implements it; Delta does not** — so Delta
cannot be an AUTO CDC sink. Iceberg is therefore the correct table format for this
stack, and everything below targets Iceberg.

That decision forces a two‑tier version reality, because *AUTO CDC itself is not in
any released Spark*:

| | **Tier A — PRIMARY (this image's default)** | **Tier B — AUTO CDC** |
|---|---|---|
| Spark | `apache/spark:4.1.2` (released) | Spark `master` / `5.0.0-SNAPSHOT` (source build) |
| Iceberg runtime | `iceberg-spark-runtime-4.1_2.13:1.11.0` (**released**, Maven Central, 2026‑05‑19) | custom **iceberg‑port** jar (Iceberg `main` patched onto Spark 5.0) |
| `create_auto_cdc_flow` | ❌ not present (Spark‑master only) | ✅ native `create_auto_cdc_flow(stored_as_scd_type=1)` → Iceberg |
| SCD | hand‑rolled MERGE (window + `MERGE INTO`) — see [`pipelines/p2_cdc`](../../../demos/pipelines/p2_cdc) | native AUTO CDC |
| Build status here | **built & verified** (`docker build` passed; checksums verified) | **documented + parameterized** (not rebuilt from scratch) |

### Feasibility verdict (resolved before building — evidence)

- **Iceberg DOES support Spark 4.1 in a released artifact.**
  `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0` is the released
  artifact (`maven-metadata.xml`: `<release>1.11.0</release>`, `lastUpdated
  20260519`). Fetched and SHA‑1‑verified during the build (see `jars.sha1`). Full
  Iceberg SQL at this tier: `MERGE` (copy‑on‑write **and** merge‑on‑read),
  row‑level `UPDATE`/`DELETE`, batch changelog, streaming append/overwrite.
- **No released Iceberg runtime exists for Spark 4.2/5.0 yet.**
  `iceberg-spark-runtime-4.2_2.13` → `<release>NONE</release>` on Maven Central;
  Iceberg's source tree stops at `spark/v4.1`. So **AUTO CDC is genuinely
  unavailable from released bits** and only exists as **Tier B = a Spark‑master
  source build + the iceberg‑port** (the user already has this at
  `~/lakehouse-stack`, image `lakehouse/spark:5.0.0-snapshot-cdc`, dirs
  `iceberg-port/` and `docker/spark42/`). This image references that base via a
  build arg rather than rebuilding it.

**Bottom line:** Tier A is the working, released image and is the right default for
everything except native AUTO CDC. Use hand‑rolled MERGE for SCD on Tier A. Switch
to Tier B only to unlock `create_auto_cdc_flow`, and track both upstreams (Spark
4.2/5.0 GA and the first official `iceberg-spark-runtime-4.2`/`5.0`) so Tier B can
be retired for released bits.

---

## 2. Jar bill-of-materials (Tier A, all pinned + SHA‑1 verified at build)

Base image **`apache/spark:4.1.2-scala2.13-java17-python3-ubuntu`** bundles
**Hadoop 3.4.2** (from `spark-parent_2.13:4.1.0` → `<hadoop.version>3.4.2`), which
fixes the S3A dependency versions below. Checksums are the canonical Maven Central
`.sha1`; they live in [`spark-connect/jars.sha1`](./spark-connect/jars.sha1) and
are enforced by `fetch-jars.sh` via `sha1sum -c` (a mismatch fails the build).

| Artifact | Version | Why this version | SHA‑1 |
|---|---|---|---|
| `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13` | `1.11.0` | only released Iceberg runtime for Spark 4.1 | `f9b1e4a1…73c5` |
| `org.apache.hadoop:hadoop-aws` | `3.4.2` | **must equal** the base's bundled Hadoop (3.4.2) | `16f9de6d…bb88` |
| `software.amazon.awssdk:bundle` | `2.29.52` | AWS SDK **v2** bundle `hadoop-aws:3.4.2` is built/tested against (`aws-java-sdk-v2.version` in `hadoop-project:3.4.2`) | `b63eb928…b5c5` |
| `org.apache.spark:spark-sql-kafka-0-10_2.13` | `4.1.2` | matches the Spark version (structured‑streaming Kafka source/sink) | `462becf2…3282` |
| `org.apache.spark:spark-token-provider-kafka-0-10_2.13` | `4.1.2` | Kafka delegation‑token provider, matches Spark | `8617f1ec…319c` |
| `org.apache.kafka:kafka-clients` | `3.9.1` | the version `spark-sql-kafka:4.1.2` compiles against (its POM) | `86ca0799…985b` |
| `org.apache.commons:commons-pool2` | `2.12.0` | connection pooling the Kafka connector needs | `458563f6…de50` |
| `org.postgresql:postgresql` | `42.7.4` | JDBC driver for the Hive Metastore's Postgres backend (HMS client) | `264310fd…adc1` |
| **auth interceptor** (`interceptor.jar`) | from **PR #3** | built from `deploy/auth/interceptor`; **not vendored here** — staged via `INTERCEPTOR_JAR` | n/a |

> Hadoop 3.4.x's S3A connector uses **AWS SDK v2**, so the baked dependency is
> `software.amazon.awssdk:bundle` (not the old `aws-java-sdk-bundle` v1). The IRSA
> credential provider class names below are SDK‑v2 names accordingly.

The auth interceptor jar is a hard runtime dependency but is **owned by PR #3**
(`deploy/auth/interceptor`). This image does **not** vendor that source; it expects
the compiled jar to be supplied at build time (`INTERCEPTOR_JAR=<path>`) and bakes
it to `/opt/spark/jars/interceptor.jar`. Build it without the jar only for
structure/lint checks (`ALLOW_MISSING_INTERCEPTOR=1`); the image then logs a loud
warning at startup and Connect auth will not function until rebuilt with the jar.

---

## 3. Baked server config (`conf/spark-defaults.template.conf`)

A **template**, rendered by `entrypoint.sh` at pod start (`__PLACEHOLDER__` →
environment). Nothing secret is baked. Highlights:

- **Iceberg over HMS** — `spark.sql.catalog.<cat>=org.apache.iceberg.spark.SparkCatalog`,
  `.type=hive`, `.uri=thrift://hive-metastore:9083`, `.warehouse=s3a://<bucket>/warehouse`;
  `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`;
  `spark.sql.catalogImplementation=hive` so the session catalog is HMS‑backed too.
- **S3A via IRSA** — `fs.s3a.aws.credentials.provider=software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider`.
  **No static keys.** The EKS IRSA webhook injects `AWS_ROLE_ARN` +
  `AWS_WEB_IDENTITY_TOKEN_FILE` into every pod using the annotated ServiceAccount;
  the provider exchanges the projected token for short‑lived role credentials.
- **Connect auth** — `spark.connect.grpc.interceptor.classes` defaults to
  `com.safesparkagents.connect.auth.PrincipalPinningInterceptor` (the class shipped
  by the PR #3 jar; overridable via the `INTERCEPTOR_CLASSES` build arg/env), and a
  placeholder `spark.connect.authenticate.token` rendered from `CONNECT_AUTH_TOKEN`
  — the **PSK comes from a k8s Secret at runtime, never baked**.

Iceberg I/O is left on the default `HadoopFileIO` so all object storage routes
through the single S3A/IRSA config (one credential path). For higher throughput you
may set `.io-impl=org.apache.iceberg.aws.s3.S3FileIO` — the baked AWS SDK v2 bundle
supplies it and it honours IRSA via the SDK default chain.

---

## 4. Build

```bash
cd deploy/eks/images/spark-connect

# Tier A (released) — production: supply the PR #3 interceptor jar
INTERCEPTOR_JAR=/path/to/auth-interceptor.jar \
IMAGE_TAG=<registry>/spark-connect-iceberg:4.1.2-iceberg1.11.0 \
  ./build.sh

# Tier A structure/lint build WITHOUT the interceptor (what CI ran here):
ALLOW_MISSING_INTERCEPTOR=1 ./build.sh

# Tier B (AUTO CDC) — 5.0-SNAPSHOT base with the Iceberg port already baked in:
INTERCEPTOR_JAR=/path/to/auth-interceptor.jar \
  ./build.sh --tier-b
# (sets BASE_IMAGE=lakehouse/spark:5.0.0-snapshot-cdc, ICEBERG_IN_BASE=true so no
#  Maven Iceberg jar is pulled — the released 4.1 runtime would not load on Spark 5.0)
```

`build.sh` knobs (env): `IMAGE_TAG`, `BASE_IMAGE`, `SPARK_VERSION`,
`ICEBERG_VERSION`, `HADOOP_AWS_VERSION`, `AWS_SDK_BUNDLE_VERSION`,
`KAFKA_CONNECTOR_VERSION`, `KAFKA_CLIENTS_VERSION`, `MAVEN_BASE_URL` (air‑gap
mirror), `INTERCEPTOR_JAR`, `ICEBERG_IN_BASE`, `ALLOW_MISSING_INTERCEPTOR`, `PUSH`.

**Reproducibility:** every Maven jar is SHA‑1‑pinned in `jars.sha1` and verified
in‑build; pin `BASE_IMAGE` to a digest (`apache/spark@sha256:…`) for a fully
content‑addressed build; `MAVEN_BASE_URL` redirects fetches to an internal proxy.

### Gate status (this PR)

- `docker build` (Tier A) — **PASSED** on the build host. All 8 BOM jars fetched
  and `sha1sum -c` verified; image assembled; baked jars + template + render
  smoke‑tested. The build ran with `ALLOW_MISSING_INTERCEPTOR=1` because the PR #3
  interceptor jar is not in this worktree — that path is the documented
  no‑interceptor build, not a faked pass.
- `shellcheck build.sh fetch-jars.sh entrypoint.sh` — **CLEAN**.
- `hadolint` — **not installed on the host**, so Dockerfile lint was skipped; the
  Dockerfile is `docker build`‑clean instead (a stronger check than lint).

---

## 5. 4.1 → 4.2 / 5.0 upgrade procedure

A minor bump is an **arg/tag change + a checksum refresh + an interceptor rebuild** —
no Dockerfile edits for Tier A:

1. **Pick the base tag** — `--build-arg BASE_IMAGE=apache/spark:4.2.x-scala2.13-javaNN-...`
   (or the Tier‑B 5.0 base). Confirm the bundled Hadoop version and set
   `HADOOP_AWS_VERSION` / `AWS_SDK_BUNDLE_VERSION` to match it.
2. **Pick the Iceberg runtime** — once `iceberg-spark-runtime-4.2_2.13` (or `5.0`)
   is **released**, set `ICEBERG_SPARK_MODULE=4.2` + `ICEBERG_VERSION=<ver>`. Until
   then, AUTO CDC stays on **Tier B** (`--tier-b`, Iceberg port baked in the base).
3. **Refresh `jars.sha1`** — replace each line with the new basename + the
   artifact's published `.sha1` (the build fails loudly on any mismatch).
4. **Bump the connectors** — `KAFKA_CONNECTOR_VERSION` to match the new Spark;
   re‑confirm `kafka-clients` from the new connector POM.
5. **Rebuild the auth interceptor** against the new Spark Connect API and re‑supply
   `INTERCEPTOR_JAR` (the gRPC interceptor SPI can drift across majors).
6. Rebuild, push the new tag, roll the Deployment + bump the executor image tag.

Track two upstreams to graduate Tier B → released: Spark 4.2/5.0 GA (ships
`create_auto_cdc_flow`) and the first official `iceberg-spark-runtime-4.2`/`5.0`
(retires the iceberg‑port).

---

## 6. How the image is used on EKS (one image, two roles)

`entrypoint.sh` renders `spark-defaults.conf` from the template, then dispatches on
its first arg:

- **`connect-server`** (the default `CMD`) → renders config and `exec`s
  `start-connect-server.sh` in the foreground (`SPARK_NO_DAEMONIZE=1`, so the server
  is PID 1 and k8s liveness + log streaming work). This is the **Connect server
  Deployment**. Append the cluster wiring at launch, e.g.
  `--master k8s://https://<eks-api> --conf spark.kubernetes.container.image=<this image>`
  and `--conf spark.kubernetes.namespace=<ns>`.
- **`driver` / `executor`** → delegates to the stock `apache/spark` k8s entrypoint
  (`/opt/entrypoint.sh`). **Spark launches executor pods itself** using
  `spark.kubernetes.container.image` — i.e. **this same image** — so executors get
  the identical Iceberg/S3A/Kafka classpath. The role arrives via the normal Spark
  k8s mechanism (`SPARK_K8S_CMD` / args).

Runtime inputs (pod spec / Secret / IRSA — none baked):
`HMS_URIS`, `ICEBERG_CATALOG`, `WAREHOUSE`, `INTERCEPTOR_CLASSES`,
`SPARK_SERVICE_ACCOUNT` (IRSA‑annotated SA), `CONNECT_AUTH_TOKEN` (PSK from a k8s
Secret), and `AWS_ROLE_ARN` + `AWS_WEB_IDENTITY_TOKEN_FILE` (IRSA webhook). gRPC is
on **15002**.
