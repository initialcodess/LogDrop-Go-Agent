#!/usr/bin/env bash
# Disposable end-to-end proof of Go tracing on kind (never a shared cluster).
#   proof/run.sh <LogDrop-Devops checkout> <output dir> [--keep]
# Builds the demo and sink images, creates kind cluster logdrop-go-proof with its own kubeconfig (no change to
# ~/.kube/config), installs the infrastructure chart's trace injector (Go needs no agent image: its pods get only the
# key file and the LOGDROP_* settings), two annotated services (the demo, one calling the other, with Redis), drives
# traffic, collects what the sink received and checks it (proof/check.py). The cluster is deleted at the end unless
# --keep is given. Nothing is pushed anywhere.
set -euo pipefail
DEVOPS=$(cd "$1" && pwd)
OUT=$(mkdir -p "$2" && cd "$2" && pwd)
KEEP=${3:-}
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PYTHON=${PYTHON:-python3}
CLUSTER=logdrop-go-proof
NS=logdrop-go-proof
export KUBECONFIG="$OUT/kubeconfig"
k() { kubectl --kubeconfig "$KUBECONFIG" --context "kind-$CLUSTER" "$@"; }
cleanup() { [ "$KEEP" = "--keep" ] || kind delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null 2>&1 || true; }
trap cleanup EXIT

docker build -q -f "$ROOT/demo/Dockerfile" -t logdrop-go-demo:dev "$ROOT" >/dev/null
docker build -q -f "$ROOT/proof/Dockerfile.sink" -t logdrop-go-sink:proof "$ROOT/proof" >/dev/null

kind get clusters | grep -qx "$CLUSTER" || kind create cluster --name "$CLUSTER" --config "$ROOT/proof/kind.yaml" --kubeconfig "$KUBECONFIG" --wait 120s
# The images are built for the machine's own architecture (the kind node's). Imported without --all-platforms, which
# `kind load` uses and which fails for images pulled for one platform only.
for image in logdrop-go-demo:dev logdrop-go-sink:proof redis:7.4-alpine busybox:1.37.0; do
  docker save "$image" | docker exec -i "$CLUSTER-control-plane" ctr --namespace=k8s.io images import --digests --snapshotter=overlayfs - >/dev/null
done
k create namespace $NS --dry-run=client -o yaml | k apply -f - >/dev/null
k create namespace logdrop-infra --dry-run=client -o yaml | k apply -f - >/dev/null
k -n $NS create secret generic logdrop-trace-ingest-key --from-literal=trace-ingest-key=ldtk_go_proof --dry-run=client -o yaml | k apply -f - >/dev/null
helm upgrade --install proof "$DEVOPS/client-helm/logdrop-infrastructure" --kubeconfig "$KUBECONFIG" --kube-context "kind-$CLUSTER" \
  -n logdrop-infra -f "$ROOT/proof/values-proof.yaml" --set cluster.uid="$(k get namespace kube-system -o jsonpath='{.metadata.uid}')" \
  --wait --timeout 180s >/dev/null
k apply -f "$ROOT/proof/apps.yaml" >/dev/null
for deploy in sink redis orders payments; do k -n $NS rollout status deploy/$deploy --timeout=180s >/dev/null; done

k -n $NS delete pod traffic --ignore-not-found >/dev/null
k -n $NS run traffic --image=busybox:1.37.0 --image-pull-policy=Never --restart=Never --command -- sh -c '
  for i in $(seq 1 10); do
    wget -q -O /dev/null --header "Authorization: Bearer caller-header-secret" --header "Cookie: session=cookie-secret" \
      "http://orders:28120/trace/demo?token=query-secret" || true
    wget -q -O /dev/null --header "User-Agent: kube-probe/1.31" "http://orders:28120/health" || true
  done' >/dev/null
k -n $NS wait --for=jsonpath='{.status.phase}'=Succeeded pod/traffic --timeout=180s >/dev/null
sleep 5   # batch delay
k -n $NS get pods -o json > "$OUT/pods.json"
k -n logdrop-infra logs deploy/proof-logdrop-infrastructure-trace-injector > "$OUT/injector.log" 2>&1 || true
for deploy in orders payments; do k -n $NS logs deploy/$deploy -c app > "$OUT/$deploy.log" 2>&1 || true; done
k -n $NS exec deploy/sink -- python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:4318/').read().decode())" > "$OUT/requests.json"
"$PYTHON" "$ROOT/proof/check.py" "$OUT/requests.json" "$OUT/pods.json"
