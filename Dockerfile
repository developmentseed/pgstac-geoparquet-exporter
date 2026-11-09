FROM python:3.12-slim@sha256:e695ab189fa2d082b1c61c69a6b888a5adeca61ceeccad107b1ed2a125721a9e AS builder

# Only the build needs uv: it stays in this stage.
# The pin has its own file because Dependabot does not read RUN lines; the uv entry in dependabot.yml bumps it.
COPY docker/requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src/ src/

# Install the locked dependencies and the project into /opt/venv
RUN UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never \
    uv sync --frozen --no-dev --no-editable

FROM python:3.12-slim@sha256:e695ab189fa2d082b1c61c69a6b888a5adeca61ceeccad107b1ed2a125721a9e

RUN apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*

# The app runs from /opt/venv and never uses the base image's pip
RUN python -m pip uninstall -y pip

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
