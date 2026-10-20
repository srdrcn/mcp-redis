# Built to run under the OpenShift restricted-v2 SCC: any UID in group 0 (/app and /etc/redis-mcp are group 0 with
# g=u), a read-only root filesystem (nothing is written at runtime: the bytecode is compiled here and uv is not
# used at runtime) and /tmp as an emptyDir.
FROM python:3.14-slim

LABEL io.modelcontextprotocol.server.name="io.github.redis/mcp-redis"

RUN pip install --upgrade uv

WORKDIR /app
COPY . /app
# UV_COMPILE_BYTECODE: write the .pyc files of the dependencies now; UV_LINK_MODE: the cache is another mount
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev \
 && /app/.venv/bin/python -m compileall -q /app/src \
 && mkdir -p /etc/redis-mcp/config /etc/redis-mcp/passwords /etc/redis-mcp/tls \
 && chgrp -R 0 /app /etc/redis-mcp \
 && chmod -R g=u /app /etc/redis-mcp

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 1001
EXPOSE 8080
# Without REDIS_MCP_CONFIG / REDIS_MCP_CONFIG_DIR, the REDIS_* environment variables describe a single cluster
# (the original behaviour, stdio). On OpenShift the Deployment runs
# `/app/.venv/bin/redis-mcp-server --config /etc/redis-mcp/config/redis-mcp.yaml` instead.
CMD ["/app/.venv/bin/python", "src/main.py"]
