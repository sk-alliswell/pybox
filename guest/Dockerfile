ARG PYTHON_IMAGE=python:3.12.15-slim-bookworm
FROM ghcr.io/astral-sh/uv:0.13.7 AS uv_tool
FROM ${PYTHON_IMAGE} AS installer
COPY --from=uv_tool /uv /usr/local/bin/uv
COPY requirements.lock /tmp/requirements.lock
RUN uv pip install --python /usr/local/bin/python3 --target /opt/dependencies \
    --only-binary :all: --require-hashes --link-mode copy -r /tmp/requirements.lock

FROM ${PYTHON_IMAGE} AS runtime
COPY --from=installer /opt/dependencies/ /usr/local/lib/python3.12/site-packages/
COPY harness.py entrypoint.py pyproject.toml uv.lock .python-version /app/
ENV SANDBOX_IMAGE_MARKER=docker-uv-agents-v6 LANGSMITH_TRACING=false PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
ENTRYPOINT ["/usr/local/bin/python3", "/app/entrypoint.py"]
