#!/usr/bin/env python3
"""Render PAPER.md into a self-contained, readable HTML page.

Reusable: re-run after editing PAPER.md to regenerate the reader.
  python3 tools/render_reader.py            # -> build/paper_reader.html
Design: light editorial-technical reading layout, sticky contents rail,
per-section maturity badges, the Section 3/4 diagrams inlined.
No external assets (fonts/JS/CSS): safe for Artifact CSP and offline use.
"""
import re, subprocess, html, sys
from pathlib import Path
try:
    import markdown
except ModuleNotFoundError:  # offline fallback: markdown-it-py where python-markdown is unavailable
    class _MarkdownItShim:
        """python-markdown-compatible facade over CommonMark (heading ids added for the toc)."""

        def __init__(self, extensions=None, extension_configs=None):
            from markdown_it import MarkdownIt

            self._md = MarkdownIt("commonmark")
            for ext in extensions or []:
                if ext == "tables":
                    self._md.enable("table")
            self._used = set()

        @staticmethod
        def _slug(text):
            import unicodedata

            slug = re.sub(r"<[^>]+>", "", text).strip().lower()
            slug = unicodedata.normalize("NFKD", slug).encode("ascii", "ignore").decode()
            slug = re.sub(r"[^\w\s-]", "", slug).strip()
            slug = re.sub(r"[\s_]+", "-", slug)
            return slug or "section"

        def convert(self, text):
            body = self._md.render(text)

            def heading_ids(match):
                tag, inner = match.group(1), match.group(2)
                base, counter = self._slug(inner), 1
                slug = base
                while slug in self._used:
                    counter += 1
                    slug = f"{base}_{counter}"
                self._used.add(slug)
                return f'<h{tag} id="{slug}">{inner}</h{tag}>'

            return re.sub(r"<h([1-6])>(.*?)</h\1>", heading_ids, body, flags=re.S)

    from types import SimpleNamespace

    markdown = SimpleNamespace(Markdown=_MarkdownItShim)

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "PAPER.md"
OUT = ROOT / "build" / "paper_reader.html"
DIAG = ROOT / "diagrams"

def git_short():
    try:
        return subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "--short", "HEAD"],
                                       text=True).strip()
    except Exception:
        return "uncommitted"

# --- section maturity (matched by leading text of each top-level heading) ---
MATURITY = [
    ("SECTION 1", "Complete", "powered study · results bound", "done"),
    ("SECTION 2", "Demonstrated", "the agent-native dev loop", "done"),
    ("SECTION 3", "Demonstrated", "five-layer tenant isolation, live on EKS", "done"),
    ("SECTION 4", "Proposed", "core demonstrated on the live platform · numbers deferred", "stub"),
    ("Appendix S2-A", "Reference spec", "executable target", "ref"),
    ("Appendix S3-A", "Reference spec", "executable target", "ref"),
]
def maturity_for(text):
    for key, label, sub, cls in MATURITY:
        if text.strip().startswith(key):
            return label, sub, cls
    return None

def load_svg(name):
    return (DIAG / name).read_text(encoding="utf-8")

md_text = SRC.read_text(encoding="utf-8")

md = markdown.Markdown(extensions=["tables", "fenced_code", "sane_lists", "attr_list", "toc"],
                       extension_configs={"toc": {"permalink": False, "toc_depth": "1-2"}})
body = md.convert(md_text)

# --- build the contents rail from h1/h2 the toc extension id'd ---
heads = re.findall(r'<h([12]) id="([^"]+)">(.*?)</h[12]>', body, flags=re.S)
def strip_tags(s): return re.sub(r"<[^>]+>", "", s)
toc_items = []
for level, hid, raw in heads:
    txt = strip_tags(raw).strip()
    cls = ""
    m = maturity_for(txt)
    if m: cls = m[2]
    toc_items.append((int(level), hid, txt, cls))
toc_html = ['<nav class="toc" aria-label="Contents">']
toc_html.append('<a class="l1 tochome" href="#overview"><span>Overview</span></a>')
for level, hid, txt, cls in toc_items:
    if level == 1:
        dot = f'<span class="dot {cls}"></span>' if cls else ""
        toc_html.append(f'<a class="l1" href="#{hid}">{dot}<span>{html.escape(txt)}</span></a>')
    else:
        toc_html.append(f'<a class="l2" href="#{hid}">{html.escape(txt)}</a>')
toc_html.append("</nav>")
toc_html = "\n".join(toc_html)

# --- badge each top-level section heading ---
def badge_h1(m):
    hid, inner = m.group(1), m.group(2)
    txt = strip_tags(inner).strip()
    mat = maturity_for(txt)
    if not mat:
        return m.group(0)
    label, sub, cls = mat
    chip = f'<span class="badge {cls}" title="{html.escape(sub)}">{html.escape(label)}</span>'
    return f'<h1 id="{hid}" class="section">{inner} {chip}</h1>'
body = re.sub(r'<h1 id="([^"]+)">(.*?)</h1>', badge_h1, body, flags=re.S)

def badge_h2(m):
    hid, inner = m.group(1), m.group(2)
    mat = maturity_for(strip_tags(inner).strip())
    if not mat:
        return m.group(0)
    label, sub, cls = mat
    chip = f'<span class="badge {cls}" title="{html.escape(sub)}">{html.escape(label)}</span>'
    return f'<h2 id="{hid}" class="section-appendix">{inner} {chip}</h2>'
body = re.sub(r'<h2 id="([^"]+)">(.*?)</h2>', badge_h2, body, flags=re.S)

# --- inline the diagrams ---
def figure(svg, cap):
    return f'<figure class="diagram"><div class="diagram-frame">{svg}</div><figcaption>{cap}</figcaption></figure>'
body = body.replace("<p>[[[SVG-SECTION3]]]</p>",
    figure(load_svg("section3_open_governed_platform.svg"),
           "The open governed reference architecture: three trust zones, untrusted agent authoring, a governed control plane, and the EKS data plane, joined by a GitOps loop, with Spark Connect as the single identity-pinned door and per-tenant isolation demonstrated on live EKS. Solid = demonstrated · dashed = configured but unrun · dotted = frontier."))
body = body.replace("<p>[[[SVG-CONNECT-K8S]]]</p>",
    figure(load_svg("section3_connect_k8s.svg"),
           "Connect on Kubernetes. A client reaches the cluster only through one external mTLS port (15009), an Envoy sidecar in the Spark Connect driver pod that pins the caller's principal and forwards over loopback to the Connect server on 127.0.0.1:15002 (never externally reachable). The driver pod is a singleton on the untainted system node pool and is the client-mode Spark driver: through an RBAC service account it schedules executor pods on a separate, tainted executor node pool (spark-role=executor), scaling 0->10 by dynamic allocation. Executors read the S3 Iceberg warehouse with the vended, prefix-scoped credentials of §3.3. Execution scales behind the governed door, never around it, and the two node pools keep driver and executors physically separated."))
body = body.replace("<p>[[[SVG-SECTION4]]]</p>",
    figure(load_svg("section4_omnigent_orchestration.svg"),
           "Omnigent: one custodian over a credential-free heterogeneous fleet. Credential custody is the keystone, now demonstrated (S4.3)."))
body = body.replace("<p>[[[SVG-CUSTODIAN]]]</p>",
    figure(load_svg("section4_custodian.svg"),
           "The credential custodian at fleet scale (S4.3), demonstrated. A fleet of credential-free agents submits inert specs and receives only pass or fail; one custodian holds and rotates every per-tenant credential, minting a fresh short-lived token per job and running the work over the §3 catalog. Credentials never cross back to the agents, and §3's per-tenant isolation holds under custody (a cross-tenant read is refused)."))
body = body.replace("<p>[[[SVG-CAPSTONE-FLEET]]]</p>",
    figure(load_svg("section4_capstone_fleet.svg"),
           "The demonstrated core on the platform's own domain (S4.5). Polly decomposes one brief into per-customer medallions, routes authoring across three vendors by difficulty, has a different vendor review, and submits each through the custodian, which runs it over that customer's own live tenant and enforces its contextual data policy. On failure the fleet repairs and converges (the §2 dev loop at fleet scale); every cross-tenant read is denied, so §3 isolation holds. The numbers are S4.6's separate study."))
body = body.replace("<p>[[[SVG-CONTAINED]]]</p>",
    figure(load_svg("section4_contained_omnigent.svg"),
           "The contained deployment shape (architecture): the Omnigent server, the custodian, and the credential-free agent fleet as pods in the client's EKS, over the Section 3 platform, with one IdP governing both. S4.5's capstone demonstrates the mechanism; this is how it is packaged for a client via the official Omnigent Kubernetes path."))
body = body.replace("<p>[[[SVG-ADVERSARY]]]</p>",
    figure(load_svg("section3_adversary_paths.svg"),
           "The adversary, and the five paths to tenant B. Each attack route is closed by exactly one of the five isolation layers, so all five must hold."))
body = body.replace("<p>[[[SVG-ISOLATION]]]</p>",
    figure(load_svg("section3_isolation_chain.svg"),
           "The five-layer per-tenant isolation chain, all demonstrated on live EKS, link by link. An agent authenticated as tenant A is routed to its own server, handed only its own credential, run on its own executor pods, authorized only for itself, and prefix-scoped at storage; tenant B travels a fully separate lane. (Proven per link; one request composing all five remains a seam, see §3.3.)"))
body = body.replace("<p>[[[SVG-REPRODUCE]]]</p>",
    figure(load_svg("section3_reproduce_flow.svg"),
           "How to reproduce the five-layer stack, from SETUP.md. Once the prerequisites are in place, four sub-deployments run inside-out (storage first, ingress last): each column maps what you run to the layer it stands up to the proof log it writes. The five logs are the per-layer evidence cited in §3.3; one composed request through all five links remains the frontier."))
body = body.replace("<p>[[[SVG-CUSTODY]]]</p>",
    figure(load_svg("section3_credential_custody.svg"),
           "Credential custody, the §3↔§4 line. The credential is vended by the catalog, held and injected by the §4 custodian, and used by the Connect server and executors; it never crosses the §2 trust boundary to the untrusted agent, which only emits an inert spec and receives a pass/fail."))
body = body.replace("<p>[[[SVG-COMPOSITION]]]</p>",
    figure(load_svg("section1_defect_composition.svg"),
           "The honest counter-signal, decomposed. Silent semantic defects shipped, per class, arm A vs arm B. The whole A−B gap is D7 (+7) and D8 (+6); the largest class, D6 dedup, is tied (a wash, not SDP-specific). D7 timezone is the sharp, skill-attributable driver, imperative ships zero and closes SDP to zero once the skill teaches the day-bucket idiom (§SM1)."))
body = body.replace("<p>[[[SVG-WHERE]]]</p>",
    figure(load_svg("section1_where_defects_caught.svg"),
           "The load-bearing result. Structural defects (D1/D4/D5) meet a boundary, before any data is processed. Bare imperative has no structural gate, so zero are caught early; four surface later at runtime, after compute is spent. SDP's framework dry-run catches 79 at that boundary before any executor starts; 30 more surface at runtime. Neither arm ships a structural defect."))
body = body.replace("<p>[[[SVG-WASTE]]]</p>",
    figure(load_svg("section1_wasted_compute.svg"),
           "The sharpest cost result (N2). Executor-seconds spent on attempts that ultimately failed: bare imperative burns 521 because spark-submit runs over the data before the fault surfaces; SDP burns ≈0.5 because its dry-run rejects the pipeline before any executor starts. Roughly 1000×; total compute ~34×. The projection scales the measured mechanism to production-sized tasks."))
body = body.replace("<p>[[[SVG-COST]]]</p>",
    figure(load_svg("cost_tokens_conciseness.svg"),
           "The cost of the declarative paradigm, arm B relative to arm A (= 100%): SDP writes far less code and spends more tokens. Bars show B as a percentage of A; absolute medians beneath."))
body = body.replace("<p>[[[SVG-HALLUCINATION]]]</p>",
    figure(load_svg("section1_hallucination_profiles.svg"),
           "What each paradigm hallucinates and where it is caught: imperative invents I/O paths (un-gateable, runtime); SDP writes imperative habits into the declarative pipeline, a large share caught at the cheap dry-run gate. Databricks DLT hallucination is zero in both arms."))
body = body.replace("<p>[[[SVG-CONTROLBOUNDARY]]]</p>",
    figure(load_svg("section2_control_boundary.svg"),
           "The control boundary, two authors. The obvious way: an imperative agent holds a live SparkSession and credentials and calls df.write.saveAsTable, so its authoring IS execution, the write crosses the boundary and touches data. The agent-native way: the SDP agent writes only @dp transforms (SparkSession.active() is a read-only handle) and produces an inert spec that stops at the boundary; absent from its surface are any owned session, credentials, endpoint, cluster config, or run lifecycle (I1/I2). Only the platform crosses, carrying the spec across through the dry-run gate and controller to the same execution engine."))
body = body.replace("<p>[[[SVG-RUNLOOP]]]</p>",
    figure(load_svg("section1_run_loop.svg"),
           "The study run-loop for one cell. Arm B passes a structural SDP dry-run before execution; arm A has no gate. Failures feed back to the agent up to an iteration cap."))
body = body.replace("<p>[[[SVG-TAXONOMY]]]</p>",
    figure(load_svg("section1_defect_taxonomy.svg"),
           "What a structural gate can and cannot catch: structural defects are caught pre-data; semantic defects are un-gateable by construction and ship as silent defects."))
body = body.replace("<p>[[[SVG-GITOPS-LOOP]]]</p>",
    figure(load_svg("section2_gitops_loop.svg"),
           "The agent-native dev loop. An untrusted agent authors an inert SDP spec (no session, credentials, or config); its only action is to open a pull request. CI gates the PR with spark-pipelines dry-run, which validates the dataflow graph without touching data (valid: Run is COMPLETED; broken: TABLE_OR_VIEW_NOT_FOUND, fed back to the agent). Only on merge does a controller, never the agent, run spark-pipelines run over Spark Connect, shipping plans to a cluster that materializes tables with the controller's credential. The same loop runs locally or on the cluster by changing SPARK_REMOTE."))
body = body.replace("<p>[[[SVG-DEVLOOP]]]</p>",
    figure(load_svg("section2_devloop_before_after.svg"),
           "The agent-native loop vs the traditional developer loop. The traditional loop is built for a trusted developer: work in an IDE holding a live session and credentials, submit to the platform with a tool (spark-submit, Jobs, or Airflow), execute over real data, and hit the error only at runtime, so every retry follows touched data and spent compute. The agent-native loop is built for an untrusted agent that holds none of that: it proposes an inert spec, a dry-run gate catches structure before any data, then a controller (never the agent) reconciles and executes; the retry happens at the gate, no data touched. Same shape, the submission tool becomes a reviewed PR."))
body = body.replace("<p>[[[SVG-DEVPROD]]]</p>",
    figure(load_svg("section2_connect_dev_prod.svg"),
           "One client, one plan, two endpoints. The client side (agent, SDP spec, dry-run gate, controller) is identical in dev and prod and holds no engine; it ships one serialized plan over gRPC to whatever SPARK_REMOTE points at. In dev that is a local Connect server (driver + executors in one in-process JVM, fast and ungoverned); in prod it is the EKS cluster (driver pod + executor pods reading the S3 warehouse through the Lakekeeper catalog behind mTLS). The executors are the only thing that touches data. Promote dev→prod by changing the URL alone."))
body = body.replace("<p>[[[SVG-CONNECT-LADDER]]]</p>",
    figure(load_svg("section2_connect_privilege.svg"),
           "The Spark Connect privilege ladder. The agent holds only an inert @dp transform (no session, credentials, or endpoint) and cannot reach the SparkContext. The controller, as a Connect client, submits serialized plans and holds the S3 credential but cannot reach SparkContext, the JVM, RDDs, or cluster config, Connect errors if it tries. Both are the client side. Below the Spark Connect boundary the cluster admin owns the engine, SparkContext, JVM, executor pods, and cluster config. A client can submit a plan but cannot cross into the engine, so SparkContext never leaves the cluster admin's side."))

# --- wrap wide tables so the page never scrolls sideways ---
body = body.replace("<table>", '<div class="tablewrap"><table>').replace("</table>", "</table></div>")

# --repo-url=<url>: rewrite in-repo relative code links (../study/, ../deploy/, ...) to absolute
# GitHub blob URLs so they resolve on the Pages site (served from /docs, above which ../ can't reach).
_repo = next((a.split("=", 1)[1] for a in sys.argv if a.startswith("--repo-url=")), None)
if _repo:
    body = body.replace('href="../', f'href="{_repo.rstrip("/")}/blob/main/')

# --- the 1-pager: a highlight card per section, linking into the full text below ---
SECTIONS = [
    ("1", "section-1-imperative-vs-sdp", "The risk, measured", "done", "Complete",
     "A controlled, pre-registered study of the same agent writing Spark pipelines two ways, imperative vs declarative (SDP).",
     ["<b>79 vs 0</b> structural defects caught at the gate before any data moves (SDP vs bare imperative)",
      "<b>~34x</b> the compute burned by imperative (<b>~1000x</b> on failed runs): a broken pipeline never starts under SDP",
      "<b>~half the code</b> (-49% lines) at ~2.3x tokens, comparable task completion",
      "the raw \"SDP looks less safe\" gap is <b>skill-induced</b>, not paradigm-inherent (timezone defects 7&rarr;0 once taught)",
      "528-run powered study, frozen instrument"]),
    ("2", "section-2-the-agent-native-development-loop", "The agent-native dev loop", "done", "Demonstrated",
     "A new inner loop for agents, propose, gate, reconcile, that closes before any data; the control boundary that enables it is what lets the agent run fully untrusted.",
     ["a new inner loop: <b>propose &rarr; dry-run gate &rarr; reconcile</b>, closing <b>before any data</b>, not the imperative write-run-find-out",
      "the enabling boundary: the agent emits only <b>inert desired-state</b>, never a session or credential, so it can be run <b>fully untrusted</b>",
      "demonstrated across hosts to <b>L3</b>; dev&rarr;prod is a <b>one-URL change</b> (Spark Connect)"]),
    ("3", "section-3-the-open-reference-architecture", "The platform, built and demonstrated", "done", "Demonstrated on live EKS",
     "An open governed stack that isolates every tenant from every other, proven layer by layer.",
     ["<b>five per-tenant isolation layers, all demonstrated on a live EKS cluster</b>",
      "an agent as tenant A <b>cannot reach tenant B by any path</b>: routing &middot; token custody &middot; execution &middot; catalog authz &middot; storage",
      "cross-tenant <b>AccessDenied</b> both directions; fleet role makes <b>0</b> warehouse data calls (CloudTrail); un-granted principal <b>403</b>",
      "only multi-tenant <b>scale</b> remains frontier"]),
    ("4", "section-4-omnigent-governed-multi-agent-orchestration-for-data-engineering", "Running it at fleet scale", "stub", "Proposed",
     "The orchestration layer over a fleet of governed agents.",
     ["<b>credential custody, demonstrated</b> (S4.3): one custodian holds + rotates every credential, a credential-free fleet, §3 isolation preserved",
      "<b>governed fleet, demonstrated on the platform</b> (S4.5): Polly built medallions for three isolated customers over their own live tenants, cross-vendor routed + reviewed, credential-free via the custodian, repaired to pass under each customer's contextual policy, every cross-tenant read denied",
      "<b>the pattern this paper was built with</b>: heterogeneous agents, adversarial cross-review, one shared governed skill set",
      "the quantitative fleet study (cost / quality numbers) is a separate experiment (S4.6)"]),
]
def scard(n, hid, title, cls, chip, blurb, bullets):
    lis = "".join(f"<li>{b}</li>" for b in bullets)
    return (f'<a class="scard {cls}" href="#{hid}">'
            f'<div class="scard-top"><span class="scard-n">Section {n}</span>'
            f'<span class="pill {cls}">{html.escape(chip)}</span></div>'
            f'<h3>{html.escape(title)}</h3><p class="scard-blurb">{blurb}</p>'
            f'<ul>{lis}</ul><span class="scard-more">Read Section {n} &rarr;</span></a>')
cards_html = ('<div class="onepager"><h2 class="onepager-h">Sections at a glance</h2>'
              '<div class="section-cards">'
              + "".join(scard(*s) for s in SECTIONS)
              + '</div><p class="onepager-foot">Two reference-spec appendices (S2-A, S3-A) are executable build targets, '
                'skim unless you are building against them. Full paper below.</p></div>')

CSS = """
<style>
:root{
  --paper:#f5f8f6; --surface:#ffffff; --panel:#eef3f0;
  --ink:#182220; --body:#26312e; --muted:#5d6b66; --faint:#8496905e;
  --rule:#dbe4e0; --rule-strong:#c4d1cc;
  --accent:#0f6e56; --accent-deep:#083f33; --accent-soft:#e3f2ec;
  --done:#3B6D11; --done-bg:#eef4e2; --scaffold:#8a5a0b; --scaffold-bg:#f7efdd;
  --stub:#534AB7; --stub-bg:#eceafb; --ref:#185FA5; --ref-bg:#e6f0fb;
  --serif:"Iowan Old Style","Charter","Cambria",Georgia,"Times New Roman",serif;
  --sans:ui-sans-serif,system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;
  --mono:ui-monospace,"SF Mono","JetBrains Mono",Menlo,Consolas,monospace;
}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--body);
  font-family:var(--serif);font-size:17px;line-height:1.62;
  -webkit-font-smoothing:antialiased;text-rendering:optimizeLegibility}
a{color:var(--accent);text-decoration:none}
a:hover{text-decoration:underline}
:focus-visible{outline:2px solid var(--accent);outline-offset:2px;border-radius:3px}

.masthead{background:linear-gradient(180deg,#ffffff, #f2f7f4);border-bottom:1px solid var(--rule);
  padding:34px 24px 26px}
.masthead .inner{max-width:1120px;margin:0 auto}
.eyebrow{font-family:var(--sans);font-size:12px;letter-spacing:.14em;text-transform:uppercase;
  color:var(--accent);font-weight:600;margin:0 0 10px}
.masthead h1.title{font-family:var(--sans);font-weight:600;font-size:30px;line-height:1.12;
  letter-spacing:-.01em;color:var(--ink);margin:0;text-wrap:balance;max-width:20ch}
.masthead .sub{font-size:16px;color:var(--muted);margin:10px 0 0;max-width:60ch}
.metarow{display:flex;flex-wrap:wrap;gap:8px 18px;align-items:center;margin-top:18px;
  font-family:var(--sans);font-size:12.5px;color:var(--muted)}
.metarow .k{font-variant-numeric:tabular-nums}
.legend{display:flex;gap:14px;flex-wrap:wrap;margin-left:auto}
.legend span{display:inline-flex;align-items:center;gap:6px}
.swatch{width:22px;height:0;border-top-width:2px;border-top-style:solid;display:inline-block}
.swatch.solid{border-color:#5d6b66}.swatch.dash{border-color:#5d6b66;border-top-style:dashed}
.swatch.dot{border-color:#5d6b66;border-top-style:dotted}

.wrap{max-width:1120px;margin:0 auto;padding:0 24px;
  display:grid;grid-template-columns:262px minmax(0,1fr);gap:44px;align-items:start}
.rail{position:sticky;top:0;max-height:100vh;overflow:auto;padding:26px 0 40px}
.rail h2{font-family:var(--sans);font-size:11px;letter-spacing:.13em;text-transform:uppercase;
  color:var(--muted);font-weight:600;margin:0 0 10px}
.toc{display:flex;flex-direction:column;gap:1px;font-family:var(--sans)}
.toc a{padding:4px 8px;border-radius:6px;color:var(--body);display:flex;align-items:center;gap:8px}
.toc a:hover{background:var(--panel);text-decoration:none}
.toc .l1{font-size:13.5px;font-weight:500;color:var(--ink);margin-top:9px}
.toc .l1.tochome{color:var(--accent);font-weight:600;margin-top:0}
.toc .l1.tochome + .l1{margin-top:14px;padding-top:11px;border-top:1px solid var(--rule)}
.toc .l2{font-size:12.5px;color:var(--muted);padding-left:20px}
.dot{width:8px;height:8px;border-radius:50%;flex:0 0 auto}
.dot.done{background:var(--done)}.dot.scaffold{background:var(--scaffold)}
.dot.stub{background:var(--stub)}.dot.ref{background:var(--ref)}

main{padding:30px 0 90px;min-width:0}
.reading-map{background:var(--surface);border:1px solid var(--rule);border-left:3px solid var(--accent);
  border-radius:10px;padding:16px 20px;margin:0 0 30px;font-size:15.5px}
.reading-map b{font-family:var(--sans);font-weight:600;font-size:12px;letter-spacing:.1em;
  text-transform:uppercase;color:var(--accent);display:block;margin-bottom:6px}
.reading-map p{margin:.4em 0}
.reading-map ol.arc{margin:.7em 0 .9em;padding-left:1.4em;display:flex;flex-direction:column;gap:.55em}
.reading-map ol.arc li{padding-left:.2em;line-height:1.5}
.reading-map ol.arc li::marker{color:var(--accent);font-family:var(--sans);font-weight:700}
.reading-map .pill{display:inline-block;margin:2px 4px 2px 0;font-family:var(--sans);font-size:12px;font-weight:500;padding:1px 7px;border-radius:20px;white-space:nowrap}
.pill.done{color:var(--done);background:var(--done-bg)}
.pill.scaffold{color:var(--scaffold);background:var(--scaffold-bg)}
.pill.stub{color:var(--stub);background:var(--stub-bg)}
.pill.ref{color:var(--ref);background:var(--ref-bg)}
.overview-lede{font-family:var(--sans);font-size:14px;color:var(--muted);margin:2px 0 22px;max-width:64ch}
/* --- the 1-pager: section highlight cards --- */
.onepager{margin:28px 0 8px}
.onepager-h{font-size:12px;letter-spacing:.15em;text-transform:uppercase;font-family:var(--sans);color:var(--muted);margin:0 0 14px;font-weight:600}
.section-cards{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px}
.scard{display:block;background:var(--surface);border:1px solid var(--rule);border-radius:12px;padding:16px 18px 15px;
  border-top:3px solid var(--rule-strong);color:var(--body);transition:border-color .15s,box-shadow .15s,transform .15s}
.scard:hover{text-decoration:none;border-color:var(--accent);box-shadow:0 8px 24px rgba(15,110,86,.11);transform:translateY(-2px)}
.scard.done{border-top-color:var(--done)}
.scard.stub{border-top-color:var(--stub)}
.scard-top{display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:2px}
.scard-n{font-family:var(--sans);font-size:11px;letter-spacing:.11em;text-transform:uppercase;color:var(--muted);font-weight:600}
.scard h3{font-family:var(--serif);font-size:19px;margin:5px 0 3px;color:var(--ink);line-height:1.18}
.scard-blurb{font-size:12.5px;color:var(--muted);margin:0 0 9px;line-height:1.45}
.scard ul{margin:0;padding-left:1.05em;display:flex;flex-direction:column;gap:5px}
.scard li{font-size:13px;line-height:1.45;color:var(--body)}
.scard li::marker{color:var(--accent)}
.scard li b{color:var(--ink)}
.scard-more{display:inline-block;margin-top:12px;font-family:var(--sans);font-size:12.5px;font-weight:600;color:var(--accent)}
.onepager-foot{font-size:12.5px;color:var(--muted);margin:15px 0 4px}
@media (max-width:720px){.section-cards{grid-template-columns:1fr}}

main :is(h1,h2,h3,h4){font-family:var(--sans);color:var(--ink);line-height:1.22;
  text-wrap:balance;font-weight:600}
h1.section{font-size:26px;letter-spacing:-.01em;margin:56px 0 6px;padding-top:22px;
  border-top:2px solid var(--rule-strong);display:flex;flex-wrap:wrap;align-items:baseline;gap:12px}
h1.section:first-of-type{border-top:none;margin-top:6px}
h2.section-appendix{font-size:22px;margin:52px 0 6px;padding-top:22px;
  border-top:2px solid var(--rule-strong);display:flex;flex-wrap:wrap;align-items:baseline;gap:12px}
main h2{font-size:19px;margin:34px 0 8px}
main h3{font-size:16px;margin:26px 0 6px;color:var(--accent-deep)}
main h4{font-size:14px;font-family:var(--sans);text-transform:uppercase;letter-spacing:.05em;
  color:var(--muted);margin:20px 0 6px}
main p{margin:.7em 0;max-width:70ch}
main li{max-width:70ch}
main ul,main ol{padding-left:22px}
main li{margin:.28em 0}
main hr{border:none;border-top:1px solid var(--rule);margin:28px 0}
strong{color:var(--ink);font-weight:600}
em{color:var(--body)}

.badge{font-family:var(--sans);font-size:12px;font-weight:600;letter-spacing:.02em;
  padding:3px 10px;border-radius:20px;line-height:1.4;white-space:nowrap;position:relative;top:-2px}
.badge.done{color:var(--done);background:var(--done-bg);border:1px solid #cfe0b0}
.badge.scaffold{color:var(--scaffold);background:var(--scaffold-bg);border:1px solid #ecd8a6}
.badge.stub{color:var(--stub);background:var(--stub-bg);border:1px solid #d5d0f2}
.badge.ref{color:var(--ref);background:var(--ref-bg);border:1px solid #c4ddf6}

blockquote{margin:16px 0;padding:12px 18px;background:var(--accent-soft);
  border-radius:8px;border:none;color:var(--accent-deep);font-family:var(--sans);font-size:15px;line-height:1.55}
blockquote p{margin:.35em 0;max-width:none}
blockquote strong{color:var(--accent-deep)}

code{font-family:var(--mono);font-size:.84em;background:var(--panel);
  padding:1.5px 5px;border-radius:4px;color:#2f3d39;word-break:break-word}
pre{background:#0f1614;color:#dbe7e2;padding:14px 16px;border-radius:10px;overflow-x:auto;
  font-size:13px;line-height:1.5}
pre code{background:none;color:inherit;padding:0;font-size:13px}

.tablewrap{overflow-x:auto;margin:16px 0;border:1px solid var(--rule);border-radius:10px}
table{border-collapse:collapse;width:100%;font-family:var(--sans);font-size:13.5px;
  font-variant-numeric:tabular-nums}
th,td{text-align:left;padding:8px 12px;border-bottom:1px solid var(--rule);vertical-align:top}
thead th{background:var(--panel);color:var(--ink);font-weight:600;white-space:nowrap;
  position:sticky;top:0}
tbody tr:last-child td{border-bottom:none}
tbody tr:hover{background:#f3f7f5}
td code{font-size:12px}

figure.diagram{margin:22px 0 26px}
.diagram-frame{background:var(--surface);border:1px solid var(--rule);border-radius:12px;
  padding:18px 20px}
.diagram-frame svg{display:block;width:100%;height:auto}
figure.diagram figcaption{font-family:var(--sans);font-size:12.5px;color:var(--muted);
  margin-top:10px;text-align:center;text-wrap:balance}

.totop{position:fixed;right:20px;bottom:20px;font-family:var(--sans);font-size:12px;
  background:var(--ink);color:#fff;padding:8px 12px;border-radius:20px;opacity:.82}
.totop:hover{opacity:1;text-decoration:none;color:#fff}

@media (max-width:900px){
  .wrap{grid-template-columns:1fr;gap:0}
  .rail{position:static;max-height:none;overflow:visible;padding:8px 0 4px;
    border-bottom:1px solid var(--rule);margin-bottom:8px}
  .rail .toc{max-height:280px;overflow:auto}
  main{padding-top:18px}
  .legend{margin-left:0}
}
@media print{.rail,.totop{display:none}.wrap{display:block}.masthead{padding:0 0 12px}}
</style>
"""

DOC = f"""{CSS}
<title>Safe, Governed AI Data Engineering: working paper</title>
<header class="masthead" id="top"><div class="inner">
  <p class="eyebrow">Working paper · internal</p>
  <h1 class="title">Safe, Governed AI Data Engineering on Spark</h1>
  <p class="sub">Can you let an AI agent write your production data pipelines without trusting it? This paper
    measures the risk, draws the boundary that makes an agent safe to run untrusted, and builds an open
    platform that enforces it, with per-tenant isolation demonstrated end to end on a live cluster.</p>
  <div class="metarow">
    <span class="k">Updated 2026-09</span><span>·</span>
    <span class="k">git {git_short()}</span><span>·</span>
    <span>4 sections + 2 reference appendices</span>
    <span class="legend">
      <span><span class="swatch solid"></span>demonstrated</span>
      <span><span class="swatch dash"></span>configured, unrun</span>
      <span><span class="swatch dot"></span>frontier</span>
    </span>
  </div>
</div></header>

<div class="wrap">
  <aside class="rail"><h2>Contents</h2>{toc_html}</aside>
  <main>
    <h1 id="overview" class="section overview-h">Overview</h1>
    <p class="overview-lede">The one-minute summary and the headline findings per section. Start here, then follow any card into the full text below.</p>
    <div class="reading-map">
      <b>What this is, in one minute</b>
      <p>AI coding agents now write real data pipelines. The failure that matters is not a crash, it is a
         pipeline that runs green and <em>silently corrupts the data</em>. So: can you get the productivity of an
         agent writing your pipelines <em>without trusting the agent</em>? This paper answers in four parts, building
         from a measurement to a running system.</p>
      <ol class="arc">
        <li><b>The risk, measured</b> (§1): a controlled study of the same agent writing pipelines imperatively versus in a declarative framework (SDP).</li>
        <li><b>The agent-native dev loop</b> (§2): a new inner loop, propose, gate, reconcile, that closes <em>before any data</em>; the control boundary that enables it lets a declarative agent run <em>fully untrusted</em>.</li>
        <li><b>The platform, built and demonstrated</b> (§3): an open governed stack that isolates every tenant from every other, on a live cluster.</li>
        <li><b>Running it at fleet scale</b> (§4): orchestration that holds credentials so the agent never sees one.</li>
      </ol>
      <p><b>Where to start:</b> the cards below give the headline findings and metrics per section and link into the full text; §1 carries the study evidence, §3 the architecture and the live isolation proof. The two reference-spec appendices are executable build targets.</p>
    </div>
    {cards_html}
    {body}
  </main>
</div>
<a class="totop" href="#top">↑ top</a>
"""

# --standalone wraps the fragment in a full HTML document (for GitHub Pages);
# a bare path arg overrides the output location.
if "--standalone" in sys.argv:
    DOC = ('<!doctype html>\n<html lang="en">\n<head>\n<meta charset="utf-8">\n'
           '<meta name="viewport" content="width=device-width, initial-scale=1">\n'
           '<title>Safe, Governed AI Data Engineering on Spark</title>\n'
           '</head>\n<body>\n' + DOC + '\n</body>\n</html>\n')

paths = [a for a in sys.argv[1:] if not a.startswith("--")]
OUTP = Path(paths[0]).expanduser() if paths else OUT
OUTP.parent.mkdir(parents=True, exist_ok=True)
OUTP.write_text(DOC, encoding="utf-8")
# Publish to the GitHub Pages source (docs/index.html) on a default build, so the live site
# cannot silently drift from the build (it did once, hence this). Skip when a custom path is given.
if not paths:
    DOCS = ROOT.parent / "docs" / "index.html"   # repo-root /docs, the GitHub Pages source
    DOCS.parent.mkdir(parents=True, exist_ok=True)
    DOCS.write_text(DOC, encoding="utf-8")
    print(f"published {DOCS} (Pages source)")
print(f"wrote {OUTP}  ({len(DOC):,} bytes, {len(toc_items)} toc entries"
      f"{', standalone' if '--standalone' in sys.argv else ''})")
