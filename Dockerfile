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

# Install uv
RUN pip install --no-cache-dir uv

# Copy project files
COPY --chown=user:user pyproject.toml .
COPY --chown=user:user src/ src/

# Install dependencies with uv (as root)
RUN uv pip install --system --no-cache -e .

# Switch to non-root user
USER user

ENTRYPOINT ["python", "-m", "pgstac_geoparquet_exporter"]
