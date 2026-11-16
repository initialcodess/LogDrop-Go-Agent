"""Checks what the proof's sink received: python3 proof/check.py <requests.json> <pods.json> (exit 0 when all pass)."""

import base64
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from otlp import decode  # noqa: E402

requests = json.loads(Path(sys.argv[1]).read_text())
pods = json.loads(Path(sys.argv[2]).read_text())["items"]
bodies = [gzip.decompress(base64.b64decode(r["body"])) if r["gzip"] else base64.b64decode(r["body"]) for r in requests]
spans = [span for body in bodies for span in decode(body)]
failed = False


def check(condition, message):
    global failed
    print(("ok   " if condition else "FAIL ") + message)
    failed = failed or not condition


app_pods = [p for p in pods if p["metadata"]["labels"].get("logdrop.io/trace") == "go"]
check(app_pods and all(p["metadata"]["annotations"].get("logdrop.io/trace-status") == "injected" for p in app_pods),
      "every annotated pod gets its settings")
orders = next(p for p in app_pods if p["metadata"]["labels"]["app"] == "orders")
env = {e["name"]: e.get("value") for e in orders["spec"]["containers"][0]["env"]}
check(not orders["spec"].get("initContainers") and "logdrop-agent" not in {v["name"] for v in orders["spec"]["volumes"]},
      "no init container and no agent volume for Go")
check(env.get("LOGDROP_INGEST_KEY_FILE") == "/var/run/secrets/logdrop.io/trace/trace-ingest-key", "the key file is mounted")
check({r["key"] for r in requests} == {"ldtk_go_proof"}, "every request carries the key from the Secret")
services = {s["resource"].get("service.name") for s in spans}
check(services == {"orders", "payments"}, f"both services report ({sorted(services)})")
by_trace = {}
for span in spans:
    by_trace.setdefault(span["trace_id"], set()).add(span["resource"]["service.name"])
check(any(len(v) == 2 for v in by_trace.values()), "a request crosses both services in one trace")
names = {s["name"] for s in spans}
check({"GET /trace/demo", "demo.sdk.operation", "demo.sdk.child", "GET"} <= names, f"server, custom and client spans ({sorted(names)})")
redis = [s for s in spans if s["attributes"].get("db.system.name", s["attributes"].get("db.system")) == "redis"]
check(bool(redis) and all(s["name"] in ("SET", "GET", "INCR") for s in redis), "Redis calls are traced, by command")
resource = next(s["resource"] for s in spans if s["resource"]["service.name"] == "orders")
check(resource.get("k8s.pod.name") == orders["metadata"]["name"] and resource.get("k8s.deployment.name") == "orders"
      and resource.get("k8s.cluster.name") == "go-proof" and resource.get("service.version") == "1.0.0"
      and resource.get("telemetry.sdk.language") == "go", "pod, workload, cluster, release and runtime identity")
check("host.name" not in resource and "process.command_line" not in resource, "no host or command line in containers")
check(not any(s["attributes"].get("http.route") == "/health" for s in spans), "kubelet-style probes are not traced")
text = b"".join(bodies)
check(all(secret not in text for secret in (b"caller-header-secret", b"cookie-secret", b"query-secret")),
      "no header, cookie or query string leaves the pod")
sys.exit(1 if failed else 0)
