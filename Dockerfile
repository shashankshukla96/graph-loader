FROM python:3.12-slim

# Prevent Python from buffering stdout and stderr
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

WORKDIR /app

# Install dependencies first for layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and default configuration
COPY src/ src/
COPY config/ config/
COPY .env.example .env.example

# Set the CLI as the default executable
ENTRYPOINT ["python", "-m", "src.cli"]
