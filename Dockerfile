# The emulator as one self-contained container: real Snowflake drivers
# connect to it unchanged, state lives in memory and dies with the container.
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

# Install first so source edits don't bust the dependency layer.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY README.md ./
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev

EXPOSE 8085
ENTRYPOINT ["uv", "run", "--no-sync", "sfemu", "serve", "--host", "0.0.0.0"]
