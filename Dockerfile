FROM python:3.11-slim

# Create non-root user
RUN useradd -m -u 1000 user && \
    mkdir -p /app && \
    chown -R user:user /app

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY --chown=user:user pyproject.toml .
COPY --chown=user:user src/ src/

# Switch to non-root user
USER user

# Install dependencies with pip
RUN pip install --no-cache-dir -e .

ENTRYPOINT ["python", "-m", "pgstac_geoparquet_exporter"]
