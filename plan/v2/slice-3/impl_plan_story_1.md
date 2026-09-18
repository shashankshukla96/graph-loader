# Implementation Plan for Story 1: Create Node Loader Dockerfile

## Files to Create/Modify
- Create: `Dockerfile.node_loader` (in project root)
- Create: `tests/test_docker_build.py`

## Logic / Contents
`Dockerfile.node_loader`:
- `FROM python:3.12-slim`
- Set `WORKDIR /app`
- Copy `requirements.txt` and run `pip install --no-cache-dir -r requirements.txt`
- Copy `src/` to `/app/src/`
- Copy `config/` to `/app/config/`
- Set environment variables: `ENV PYTHONPATH=/app` and `ENV PYTHONUNBUFFERED=1`
- Set `ENTRYPOINT ["python", "-m", "src.loader.node_loader"]`

`tests/test_docker_build.py`:
- Use `docker.from_env()` to build the image from `Dockerfile.node_loader`.
- Must be decorated with `@pytest.mark.integration` so it doesn't break standard lightweight unit test runs.
- Verify the build succeeds and the entrypoint is correct.

## Rollback
- Delete `Dockerfile.node_loader` and `tests/test_docker_build.py`.
