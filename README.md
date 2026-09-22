# Safe, Governed AI Data Engineering on Spark

A working paper  (and its full apparatus) on whether the **authoring paradigm** an AI agent is given
changes how *safely* it writes Spark data pipelines, and the control-boundary and open-platform
architecture that result motivates. Everything here is self-contained: paper, code, and reproduction.

- 📄 **Read the paper**: [`paper/PAPER.md`](paper/PAPER.md) · rendered reading site: **[`docs/index.html`](docs/index.html)** (GitHub Pages)
- 🔬 **Reproduce it**: [`reproduce/REPRODUCE.md`](reproduce/REPRODUCE.md) + [`reproduce/ENV_SETUP.md`](reproduce/ENV_SETUP.md), byte-identical, built to re-run against future Spark releases
- 🧪 **Study code**: [`study/`](study/), harness, arms, frozen corpus/seeds, and the committed result files behind every number
- 🏗️ **Reference architecture**: [`deploy/`](deploy/) the governed Spark-Connect-on-Kubernetes platform (Sections 3–4)

## What the study found

A controlled experiment (528 runs; 22 tasks × 12 seeds × 2 arms; `claude-opus-4-8`) varying **only the
paradigm**: **A** = bare imperative PySpark, **B** = Spark Declarative Pipelines (SDP) with its intrinsic
structural dry-run:

- **Structural safety**: SDP's dry-run intercepts **79** structural defects *before any data is
  processed*, against **0** for imperative (which surfaces them at runtime).
- **Silent defects**: a raw residue that appears to favor imperative is **skill-induced, not
  paradigm-inherent**: its main driver (timezone/day-bucket errors) collapses **7 → 0** once the SDP
  skill teaches a UTC idiom.
- **Cost**: SDP writes **~half the code** (−49% lines) at **~2.3× the tokens**, with comparable task
  completion.

*Headline:* declarative structure buys an early, real safety margin on structural faults, and is not by
itself less safe on semantic ones, provided it is paired with a paradigm-matched skill.

## Layout

| path | what |
|---|---|
| [`paper/`](paper/) | the paper (`PAPER.md`), figures (`diagrams/`), the reader generator (`tools/render_reader.py`), working notes |
| [`docs/`](docs/) | the rendered static site (GitHub Pages) |
| [`reproduce/`](reproduce/) | reproduction index + environment setup + link to the raw-run archive |
| [`study/`](study/) | the experiment: harness, arms, skills, prompts, pilot, GitOps demo, tests |
| [`study/config/`](study/config/) | frozen corpus + seeds + controlled config + published row schema |
| [`study/results/`](study/results/) | committed result files (and env/quarantine sidecars) behind every number |
| [`study/reporting/`](study/reporting/) | POWERED / SUPPLEMENTAL headline and report artifacts |
| [`defect_battery/`](defect_battery/) | the single-source grading oracle (`quantify*.py`) + E3 defect variants the blind grader imports (deliberately kept at the root) |
| [`generators/`](generators/) | deterministic NDJSON data generators (per `(task, seed)` byte-identical input) |
| [`demos/`](demos/) | five runnable demos + the `pipelines/p1–p5` SDP specs they exercise |
| [`connect/`](connect/) | Spark Connect client + smoke test + local launcher scripts + bundled Kafka connector jars |
| [`deploy/`](deploy/) | the governed Connect-on-Kubernetes reference architecture (Terraform, k8s, systemd) |
| [`config/`](config/) | `aws.env.example`: copy to `config/aws.env` (gitignored) and fill in your own AWS values |

## Configuration & safety

No account-specific AWS values are committed. The platform code and runbooks reference
`${AWS_ACCOUNT_ID}`, `${WAREHOUSE_BUCKET}`, `${EKS_CLUSTER}`, `${RDS_ENDPOINT}`, etc.; copy
[`config/aws.env.example`](config/aws.env.example) to `config/aws.env` (gitignored) and supply your own, 
see [`reproduce/ENV_SETUP.md`](reproduce/ENV_SETUP.md).

## Status

Working paper. **Section 1** (the study) is complete and powered; **Section 2** (the control boundary) is
demonstrated to layer L3; **Sections 3–4** (the open platform and fleet orchestration) are a locked
scaffold and a thesis stub, with honest gaps marked throughout. Every number cites a committed result
file, and the full raw run is archived for exact replay.
