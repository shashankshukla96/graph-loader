FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Generated from pyproject.toml; install runtime dependencies before source.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Isolated setuptools reads the literal file version; no Git or path rescue.
COPY pyproject.toml PACKAGE_README.md LICENSE MANIFEST.in ./
COPY src/ src/
RUN pip install --no-cache-dir --no-deps .
COPY config/ config/
COPY .env.example .env.example

ENTRYPOINT ["python", "-m", "neo4j_graph_loader.cli"]
