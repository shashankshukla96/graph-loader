# Implementation Plan: Story 3 — Dockerfile & Full Stack E2E Validation

## Objective
Package the Python CLI and loader infrastructure into a Docker image, establishing the deployment artifact for the project.

## Files to Create
| File | Change |
|---|---|
| `Dockerfile` | Create container build instructions |

## Key Logic

**Dockerfile Structure:**
```dockerfile
FROM python:3.12-slim

WORKDIR /app

# Install dependencies first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and default configuration
COPY src/ src/
COPY config/ config/
COPY .env.example .env.example

# Ensure Python can resolve 'src' as a module
ENV PYTHONPATH=/app

# Set the CLI as the default executable
ENTRYPOINT ["python", "-m", "src.cli"]
```

## Validation Commands
```bash
# Build the image
docker build -t graph-loader .

# Verify the entrypoint and CLI routing
docker run --rm graph-loader --help
docker run --rm graph-loader start --help
```

## Rollback
Delete `Dockerfile` and `plan/v1/slice-5/impl_plan_story_3.md`.
