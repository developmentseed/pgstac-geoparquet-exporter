FROM python:3.11-slim AS builder

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src/ src/

# Install the locked dependencies and the project into /opt/venv
RUN UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never \
    uv sync --frozen --no-dev --no-editable --no-cache

FROM python:3.11-slim

# The app runs from /opt/venv and never uses the base image's packaging tools
RUN python -m pip uninstall -y pip setuptools wheel

# Create non-root user
RUN useradd -m -u 1000 user && \
    mkdir -p /app && \
    chown -R user:user /app

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Switch to non-root user
USER user

ENTRYPOINT ["python", "-m", "pgstac_geoparquet_exporter"]
