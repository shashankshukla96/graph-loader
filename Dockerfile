FROM python:3.12-slim

# E14 single container: tini is PID 1 so `start --runtime process` can run every role as a child process of the CLI. It forwards
# SIGTERM (docker stop, pod termination) to the CLI only (no `-g`: the CLI performs the ordered stop itself and signals its own
# process groups), reaps orphans (planner workers, role grandchildren) so no zombie accumulates, and exits with the CLI's status.
# The Debian package is used instead of a downloaded release binary so the install is one signed apt transaction for amd64 and
# arm64 alike, with no network fetch outside the distribution mirrors and no checksum to carry per architecture. The pin is the
# upstream version, asserted below (a base image that ships another tini fails the build loudly); the Debian package revision is
# whatever the base image's release carries, so byte reproducibility is bounded by the unpinned base tag, as it already was.
ARG TINI_VERSION=0.19.0
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/* \
    && tini --version | grep -F "tini version ${TINI_VERSION}"

# The candidate builder supplies the literal package version; unlabeled ad-hoc
# development builds are not selectable release candidates.
ARG GRAPH_LOADER_VERSION
LABEL org.opencontainers.image.version=$GRAPH_LOADER_VERSION \
      org.opencontainers.image.source="https://github.com/shashankshukla96/graph-loader" \
      io.graph-loader.component="cli" \
      io.graph-loader.image-contract="1"

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Generated from pyproject.toml; install runtime dependencies before source.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Isolated setuptools reads the literal file version; no Git or path rescue.
COPY pyproject.toml PACKAGE_README.md LICENSE MANIFEST.in ./
COPY src/neo4j_graph_loader/ src/neo4j_graph_loader/
RUN pip install --no-cache-dir --no-deps .
RUN python -c 'import importlib.metadata as m, sys; assert not sys.argv[1] or m.version("neo4j-graph-loader") == sys.argv[1], "image/package version mismatch"' "$GRAPH_LOADER_VERSION"
COPY config/ config/
COPY .env.example .env.example

# Exec form; the CLI's arguments (CMD or `docker run ... start ...`) still follow the module name, exactly as before.
ENTRYPOINT ["tini", "--", "python", "-m", "neo4j_graph_loader.cli"]
