# Implementation Plan: Story 2 - Neo4j 5 Enterprise Docker Service

## Files to Create/Modify
- `J:\Graph Loader\docker-compose.yaml` (Create)

## New Classes and Methods
- N/A

## Key Logic / Implementation Details
- Create the Story 2 version of `docker-compose.yaml` with only the `neo4j` service using the `neo4j:5-enterprise` image; Kafka is deliberately added by Story 3 in a later commit.
- Map ports 7474 (HTTP) and 7687 (Bolt).
- Define the required environment contract exactly: `NEO4J_AUTH: "${NEO4J_USERNAME:-neo4j}/${NEO4J_PASSWORD}"`, `NEO4J_ACCEPT_LICENSE_AGREEMENT: "${NEO4J_ACCEPT_LICENSE_AGREEMENT:-yes}"`, `NEO4J_server_memory_heap_initial__size: "512m"`, `NEO4J_server_memory_heap_max__size: "2G"`, `NEO4J_server_memory_pagecache_size: "1G"`, `NEO4J_dbms_security_procedures_unrestricted: "apoc.*"`, `NEO4J_dbms_security_procedures_allowlist: "apoc.*"`, and `NEO4J_PLUGINS: '["apoc"]'`. The source story's legacy `NEO4J_dbms_memory_pagecache__size` name is deliberately corrected: the current Neo4j Docker configuration reference maps `server.memory.pagecache.size` to `NEO4J_server_memory_pagecache_size`, and the legacy key prevents the Neo4j 5 container from starting.
- Add Compose comments explaining that Enterprise is needed for planned concurrent-transaction and lock-optimization behavior, the 512m/2G/1G memory profile supports local development datasets up to roughly 10M nodes, APOC is required for the planned `apoc.lock.nodes()` deadlock-prevention strategy, and automatic APOC installation needs internet on the first container start.
- Define volumes for data and logs (`neo4j_data`, `neo4j_logs`).
- Define the `graph-loader-net` bridge network.
- Include the required health check using `wget -q --spider http://localhost:7474`, with a 10-second interval, 5-second timeout, 10 retries, and a 30-second start period.
- Preserve persistent data by never running `docker compose down -v` during validation. A failed validation is cleaned up with `docker compose down` only.

## Tests to Write
- N/A (end-to-end smoke tests are deferred to Story 5). Validate this infrastructure story by running `docker compose config --quiet`, then `docker compose up -d neo4j`.
- Require the Compose health check to report `healthy` within 60 seconds, verify `http://localhost:7474` responds, and use the Neo4j Python driver to run `RETURN 1` and `RETURN apoc.version()` against Bolt using the configured environment credentials. If health arrives after 60 seconds, retain logs for diagnosis but do not mark the acceptance criterion as passed.
- Record container logs on failure and leave the successfully validated service available for dependent stories.

## Rollback / Cleanup
- Remove `docker-compose.yaml` and tear down the container if the implementation fails.
