#!/usr/bin/env bash
# Runs integration/ against real PostgreSQL, Redis, Kafka and RabbitMQ in Docker, then removes the containers, the
# network and the images this script pulled (images that were already there stay).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
NET=logdrop-go-it
IMAGES=(postgres:17-alpine redis:7.4-alpine apache/kafka:4.3.2 rabbitmq:3-management)
PULLED=()
for image in "${IMAGES[@]}"; do
  docker image inspect "$image" >/dev/null 2>&1 || PULLED+=("$image")
done
cleanup() {
  docker rm -f logdrop-go-it-pg logdrop-go-it-redis logdrop-go-it-kafka logdrop-go-it-rabbit >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
  for image in "${PULLED[@]+"${PULLED[@]}"}"; do docker rmi "$image" >/dev/null 2>&1 || true; done
}
trap cleanup EXIT
docker network create "$NET" >/dev/null
docker run -d --name logdrop-go-it-pg --network "$NET" -p 127.0.0.1:15440:5432 \
  -e POSTGRES_USER=it -e POSTGRES_PASSWORD=it-local-only -e POSTGRES_DB=it postgres:17-alpine >/dev/null
docker run -d --name logdrop-go-it-redis --network "$NET" -p 127.0.0.1:16390:6379 redis:7.4-alpine >/dev/null
docker run -d --name logdrop-go-it-kafka --network "$NET" -p 127.0.0.1:29092:29092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:29092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:29092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:4.3.2 >/dev/null
docker run -d --name logdrop-go-it-rabbit --network "$NET" -p 127.0.0.1:15690:5672 rabbitmq:3-management >/dev/null
echo "waiting for the servers"
for _ in $(seq 1 60); do
  # RabbitMQ is not probed with docker exec: a CLI run as root before the server starts writes its Erlang cookie
  # with the wrong owner and the server stops. The test retries its connection instead.
  docker exec logdrop-go-it-pg pg_isready -U it -h 127.0.0.1 >/dev/null 2>&1 \
    && docker logs logdrop-go-it-kafka 2>&1 | grep -q "Kafka Server started" && break
  sleep 2
done
cd "$ROOT/integration"
LOGDROP_IT=1 \
LOGDROP_IT_POSTGRES="postgres://it:it-local-only@127.0.0.1:15440/it?sslmode=disable" \
LOGDROP_IT_REDIS=127.0.0.1:16390 \
LOGDROP_IT_KAFKA=127.0.0.1:29092 \
LOGDROP_IT_RABBITMQ="amqp://guest:guest@127.0.0.1:15690/" \
  go test -count=1 -v -run TestIntegrations ./... 2>&1 | grep -E "^(=== RUN|--- |    |ok|FAIL|PASS|panic)" | tee "${TMPDIR:-/tmp}/logdrop-go-it.log" || true
if grep -q "^FAIL" "${TMPDIR:-/tmp}/logdrop-go-it.log"; then
  docker ps -a --filter name=logdrop-go-it --format '{{.Names}} {{.Status}}'
  docker logs --tail 15 logdrop-go-it-rabbit 2>&1 | sed 's/^/rabbitmq: /'
  exit 1
fi
