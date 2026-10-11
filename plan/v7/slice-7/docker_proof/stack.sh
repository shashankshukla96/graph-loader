#!/bin/bash
# v7-S7 scratch stack: p0rh-net, p0rh-kafka (cpuset $V7S7_KAFKA_CPUSET, default 30), p0rh-neo4j 5-enterprise + APOC (2G heap, 2G page
# cache, cpuset $V7S7_NEO4J_CPUSET default 26-29, query log on the host). usage: stack.sh up|down   (touches only $V7S7_PREFIX-* objects, default p0rh-*)
# State (password, query log, run outputs): $V7S7_STATE, default /mnt/graph-loader/bench/p0/v7s7.
set -eu
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
. "$HERE/env.sh"
case ${1:?up|down} in
up)
  mkdir -p "$STATE" "$QLOG_DIR"; chown -R 7474:7474 "$QLOG_DIR" 2>/dev/null || sudo chown -R 7474:7474 "$QLOG_DIR"
  umask 077; echo "export NEO4J_PASSWORD=$(python3 -c 'import secrets;print(secrets.token_hex(8))')" > "$STATE/.pw"; umask 022
  . "$STATE/.pw"
  docker network create "$PFX-net" >/dev/null
  docker run -d --name "$PFX"-kafka --network "$PFX-net" --cpuset-cpus "$V7S7_KAFKA_CPUSET" -m 4g -p "$KAFKA_PORT":9092 \
    -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@"$PFX"-kafka:29093 \
    -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:29093,INTERNAL://0.0.0.0:29092 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:$KAFKA_PORT,INTERNAL://"$PFX"-kafka:29092 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=false \
    -e CLUSTER_ID=djdTNFByb29mU3RhY2s3Nw -e "KAFKA_HEAP_OPTS=-Xms1g -Xmx1g" \
    confluentinc/cp-kafka:7.7.0 >/dev/null
  docker run -d --name "$PFX"-neo4j --network "$PFX-net" --cpuset-cpus "$V7S7_NEO4J_CPUSET" -p "$NEO4J_PORT":7687 \
    -e NEO4J_AUTH=neo4j/$NEO4J_PASSWORD -e NEO4J_ACCEPT_LICENSE_AGREEMENT=yes \
    -e NEO4J_server_memory_heap_initial__size=2G -e NEO4J_server_memory_heap_max__size=2G \
    -e NEO4J_server_memory_pagecache_size=2G \
    -e NEO4J_server_logs_config=/graph-loader-conf/server-logs.xml \
    -e "GL_QUERY_LOG_ROTATION_SIZE=512 MB" -e GL_QUERY_LOG_KEEP_FILES=20 \
    -e NEO4J_db_logs_query_parameter__logging__enabled=true \
    -e 'NEO4J_dbms_security_procedures_unrestricted=apoc.*' -e 'NEO4J_dbms_security_procedures_allowlist=apoc.*' \
    -e 'NEO4J_PLUGINS=["apoc"]' \
    -v "$QLOG_DIR":/logs -v "$ROOT/config/neo4j/server-logs.xml":/graph-loader-conf/server-logs.xml:ro \
    "${V7S7_NEO4J_IMAGE:-neo4j:5-enterprise}" >/dev/null
  until cyp "SHOW DATABASES YIELD name" system >/dev/null 2>&1; do sleep 2; done
  until kx kafka-topics --bootstrap-server "$PFX"-kafka:29092 --list >/dev/null 2>&1; do sleep 2; done
  endpoints; echo "stack up"
  ;;
down)
  docker rm -f "$PFX"-kafka "$PFX"-neo4j >/dev/null 2>&1 || true
  docker network rm "$PFX-net" >/dev/null 2>&1 || true
  echo "stack down"
  ;;
esac
