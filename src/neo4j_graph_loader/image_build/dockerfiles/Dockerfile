# Built from a generated context (see docs/RESTRICTED_NETWORKS.md): the package wheel, the hashed dependency lock, install.py and,
# optionally, a wheelhouse and a private CA bundle. Nothing here reaches a public host unless you point it at one.
#
#   BASE_IMAGE            the base image, ideally a digest of your own mirror (default: python:3.12-slim)
#   PACKAGE_WHEEL         file name of the package wheel in the context, and PACKAGE_WHEEL_SHA256 its digest
#   DEPENDENCY_SOURCE     index-secret (the URL is a BuildKit secret `pip_index`), index-url (INDEX_URL, no credentials) or wheelhouse
#   GRAPH_LOADER_VERSION  the package version (labelled, and checked against the installed distribution)
#   INSTALL_FLAGS         extra install.py flags for checks (for example --probe-cmdline)
# No credential is ever a build argument. The Python dependency set is the hashed lock; the base image's Debian packages are not pinned.
ARG BASE_IMAGE=python:3.12-slim
FROM ${BASE_IMAGE}

# tini is PID 1 so `start --runtime process` can run every role as a child process of the CLI. It forwards SIGTERM (docker stop, pod
# termination) to the CLI only (no `-g`: the CLI performs the ordered stop itself and signals its own process groups), reaps orphans (planner
# workers, role grandchildren) so no zombie accumulates, and exits with the CLI's status. The Debian package is used instead of a downloaded
# release binary so the install is one signed apt transaction for amd64 and arm64 alike. A base image that already carries tini
# ${TINI_VERSION} skips apt entirely (an image with no route to Debian needs nothing here). Otherwise DEBIAN_MIRROR, when set, replaces the
# Debian sources with that mirror (the same layout as deb.debian.org/debian, signed by the Debian archive keys). The pin is the upstream
# version, asserted below; the Debian package revision is whatever the mirror carries and is recorded by the build, not pinned.
ARG TINI_VERSION=0.19.0
ARG DEBIAN_MIRROR=
RUN set -eu; \
    if tini --version 2>/dev/null | grep -qF "tini version ${TINI_VERSION}"; then \
        echo "tini ${TINI_VERSION} is already in the base image; apt is skipped"; \
    else \
        if [ -n "${DEBIAN_MIRROR}" ]; then \
            . /etc/os-release; \
            rm -f /etc/apt/sources.list /etc/apt/sources.list.d/*; \
            printf 'Types: deb\nURIs: %s\nSuites: %s %s-updates\nComponents: main\nSigned-By: /usr/share/keyrings/debian-archive-keyring.gpg\n' \
                "${DEBIAN_MIRROR}" "${VERSION_CODENAME}" "${VERSION_CODENAME}" > /etc/apt/sources.list.d/debian.sources; \
        fi; \
        apt-get update; \
        apt-get install -y --no-install-recommends tini; \
        rm -rf /var/lib/apt/lists/*; \
    fi; \
    tini --version | grep -F "tini version ${TINI_VERSION}"

ARG GRAPH_LOADER_VERSION
LABEL org.opencontainers.image.version=$GRAPH_LOADER_VERSION \
      org.opencontainers.image.source="https://github.com/Neburyx/graph-loader" \
      io.graph-loader.component="cli" \
      io.graph-loader.image-contract="1"

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# The package wheel and its locked dependencies, installed by the helper with pip started in a fresh environment (see install.py). The
# context is bind-mounted, so neither the wheel, the lock nor any wheelhouse becomes a layer; the index URL arrives only as the BuildKit
# secret `pip_index`, which pip reads from the environment of that one process and which never reaches an argument list or a layer.
ARG PACKAGE_WHEEL
ARG PACKAGE_WHEEL_SHA256
ARG DEPENDENCY_SOURCE=index-secret
ARG INDEX_URL=
ARG INSTALL_FLAGS=
RUN --mount=type=bind,source=.,target=/ctx \
    --mount=type=secret,id=pip_index,required=false \
    set -eu; \
    case "${DEPENDENCY_SOURCE}" in \
        index-secret) source="--index-secret /run/secrets/pip_index" ;; \
        index-url)    source="--index-url ${INDEX_URL}" ;; \
        wheelhouse)   source="--wheelhouse /ctx/wheelhouse" ;; \
        *) echo "DEPENDENCY_SOURCE must be index-secret, index-url or wheelhouse" >&2; exit 2 ;; \
    esac; \
    python -I /ctx/install.py --lock /ctx/lock.txt --wheel "/ctx/${PACKAGE_WHEEL}" --wheel-sha256 "${PACKAGE_WHEEL_SHA256}" \
        --ca-bundle /ctx/pip-ca.pem ${INSTALL_FLAGS} ${source}
RUN python -I -c 'import importlib.metadata as m, sys; assert not sys.argv[1] or m.version("neo4j-graph-loader") == sys.argv[1], "image/package version mismatch"' "$GRAPH_LOADER_VERSION"

# The bundled canonical schema is the default --config of the role entry points (relative path config/graph_schema.yaml).
RUN mkdir -p config && python -I -c 'import importlib.resources as r, shutil; shutil.copyfile(r.files("neo4j_graph_loader").joinpath("data", "graph_schema.yaml"), "config/graph_schema.yaml")'

ENTRYPOINT ["tini", "--", "python", "-m", "neo4j_graph_loader.cli"]
