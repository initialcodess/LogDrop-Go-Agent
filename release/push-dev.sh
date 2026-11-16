#!/usr/bin/env bash
# Builds the demo image for linux/amd64 (the dev cluster) from a clean checkout and pushes it to the dev registry only
# (registry.digitalocean.com/logdrop-dev-fra1). Go has no agent image: the demo builds the logdrop package in. Nothing
# is deployed: the dev values in LogDrop-Devops pick an image up only when its digest is written there. Prints the
# digest to pin.
#   release/push-dev.sh            # needs doctl logged in to the LogDrop DigitalOcean team
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
REGISTRY=registry.digitalocean.com/logdrop-dev-fra1
[ -z "$(git -C "$ROOT" status --porcelain)" ] || { echo "commit or stash first: images are built from HEAD" >&2; exit 1; }
COMMIT=$(git -C "$ROOT" rev-parse --short=12 HEAD)
# A short-lived read-write token in a throwaway Docker config; the user's own Docker config is not touched.
export DOCKER_CONFIG; DOCKER_CONFIG=$(mktemp -d)
trap 'rm -rf "$DOCKER_CONFIG"' EXIT
doctl registry docker-config --read-write --expiry-seconds 900 > "$DOCKER_CONFIG/config.json"
[ -e ~/.docker/cli-plugins/docker-buildx ] && mkdir -p "$DOCKER_CONFIG/cli-plugins" && ln -s ~/.docker/cli-plugins/docker-buildx "$DOCKER_CONFIG/cli-plugins/"
ref="$REGISTRY/logdrop-go-demo:dev-$COMMIT"
# One image manifest, no index with a provenance attestation.
docker build -q --platform linux/amd64 --provenance=false --sbom=false -f "$ROOT/demo/Dockerfile" -t "$ref" "$ROOT" >/dev/null
docker push -q "$ref" >/dev/null
echo "logdrop-go-demo dev-$COMMIT $(docker buildx imagetools inspect "$ref" --format '{{json .Manifest.Digest}}' | tr -d '"')"
