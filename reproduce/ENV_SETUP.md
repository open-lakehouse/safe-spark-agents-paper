# Environment setup

Everything needed to run the study and (optionally) stand up the reference platform. The study's
**safety, token, and conciseness** results run locally with no cloud; only the **data-compute (H3)**
measurement needs the EKS cluster.

## 1. Toolchain
- **Python** 3.12+ (the reader/analysis) and a **JDK 17** on `JAVA_HOME` for Spark. Java 17 may be
  installed while `JAVA_HOME` is unset; export it once before any local Spark launch:
  `export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))` (dry-run gate demos and the
  analysis recompute do not need it; a live Spark or Connect launch does).
- **Apache Spark 4.1.x** with `pyspark` (Spark Connect + Declarative Pipelines). Local runs use a
  local Spark / local Spark Connect server; no Databricks.
- For the platform only: `terraform`, `aws`, `kubectl`, `kustomize`, `docker`.
- Python packages for the reader: `pip install markdown` (already stdlib otherwise).

## 2. Secrets & config (never committed)
- **`ANTHROPIC_API_KEY`**: export in your shell for any live agent run.
- **AWS / infra values**: copy the template and fill in your own; the file is gitignored and the
  platform code references it as `${VARS}` (no account-specific value is in the tree):
  ```bash
  cp config/aws.env.example config/aws.env
  $EDITOR config/aws.env            # AWS_ACCOUNT_ID, WAREHOUSE_BUCKET, EKS_CLUSTER, RDS_ENDPOINT, ...
  set -a && source config/aws.env && set +a
  ```
  Manifests/terraform that contain `${AWS_ACCOUNT_ID}` etc. are rendered with your values via
  `envsubst` (or your own templating) at apply time, see `deploy/eks/RUNBOOK.md`.

## 3. Run the study (local, no cloud)
```bash
cd study
# recompute the paper's numbers from committed results (no LLM, no Spark):
python3 analysis/analyze.py results/results.powered.AB.n12.final.jsonl --tasks config/TASKS.lock.json --assume-backend local
# re-run agents (needs ANTHROPIC_API_KEY + a local Spark Connect endpoint):
#   see study/repro/REPRODUCE.md for the exact runner.py invocation
```

## 4. Stand up the reference platform (optional, for H3 / Sections 2–3)
Source `config/aws.env` first, then follow [`deploy/eks/RUNBOOK.md`](../deploy/eks/RUNBOOK.md) (Terraform →
image build/push to `${ECR_REGISTRY}` → `kustomize`/`kubectl` apply → connect through the mTLS ingress).
The H3 compute run is documented in [`study/repro/h3_eks/`](../study/repro/h3_eks/).

## 5. Raw run archive (byte-identical replay)
The full transcripts + generated data + results are a GitHub **Release** asset (not in the tree). See
[`REPRODUCE.md`](REPRODUCE.md) §3 to download and extract it into `study/`.
