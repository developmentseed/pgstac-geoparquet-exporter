FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b AS builder

# psycopg's C module compiles against Debian's libpq; the compiler stays in this stage
RUN apt-get update && apt-get install -y --no-install-recommends gcc libc6-dev libpq-dev

# Only the build needs uv: it stays in this stage.
# The pin has its own file because Dependabot does not read RUN lines; the uv entry in dependabot.yml bumps it.
COPY docker/requirements.txt /tmp/requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src/ src/

# Install the locked dependencies and the project into /opt/venv,
# with psycopg's C module instead of the psycopg-binary wheel and its bundled libpq
RUN UV_PROJECT_ENVIRONMENT=/opt/venv UV_PYTHON_DOWNLOADS=never \
    uv sync --frozen --no-dev --no-editable --group image --no-install-package psycopg-binary

FROM python:3.12-slim@sha256:5e2bdb1b0d542d5039cffb8181c65930093949194a65c365dfb1b0fe13a4fa5b

RUN apt-get update && apt-get upgrade -y \
    && apt-get install -y --no-install-recommends libpq5 \
    && rm -rf /var/lib/apt/lists/*

# The app runs from /opt/venv and never uses the base image's pip
RUN python -m pip uninstall -y pip

# Create non-root user
RUN useradd -m -u 1000 user && \
    mkdir -p /app && \
    chown -R user:user /app

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
# Fail at import instead of falling back to pure-Python psycopg if the C module is missing
ENV PSYCOPG_IMPL=c

# Switch to non-root user
USER user

ENTRYPOINT ["python", "-m", "pgstac_geoparquet_exporter"]
