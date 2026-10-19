# Stage 1: Build qlcp artifacts through the Hatchling install hook
FROM python:3.13-alpine3.22@sha256:ab45bd32143151fe060d48218b91df43a289166e72ec7877823b1c972580bed3 AS protocol-builder

RUN apk add --no-cache gcc musl-dev cmake make git

COPY --from=ghcr.io/astral-sh/uv@sha256:9a23023be68b2ed09750ae636228e903a54a05ea56ed03a934d00fe9fbeded4b /uv /uvx /bin/

WORKDIR /app

# Cache runtime Python dependencies in the builder stage before any qlcp or app
# source is copied. This layer only needs to rerun when pyproject.toml or
# uv.lock changes, and it does not install ctl-vector.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY pyproject.toml uv.lock hatch_build.py ./
COPY ctl-qlcp-lib/ ./ctl-qlcp-lib/

# Hatchling package discovery needs the package directories to exist, but the
# protocol build should stay cached independently from normal app source edits.
RUN mkdir -p src/vector && \
    touch src/vector/__init__.py

# Install ctl-vector in the builder stage. This is the only Docker build
# step that intentionally runs the Hatchling protocol build hook.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Stage 2: Final runtime image
FROM python:3.13-alpine3.22@sha256:ab45bd32143151fe060d48218b91df43a289166e72ec7877823b1c972580bed3

COPY --from=ghcr.io/astral-sh/uv@sha256:9a23023be68b2ed09750ae636228e903a54a05ea56ed03a934d00fe9fbeded4b /uv /uvx /bin/
WORKDIR /app

# Cache runtime Python dependencies in the final image before copying app
# source. git is needed only while uv fetches git-sourced dependencies.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    apk add --no-cache --virtual .git-deps git && \
    uv sync --locked --no-install-project --no-dev && \
    apk del .git-deps

RUN mkdir -p recordings

# Install opus and ffmpeg for audio recording
RUN apk add --no-cache opus opus-dev ffmpeg

# opuslib doesn't find the library correctly on alpine, so we have to patch it to use the correct name
RUN sed -i 's/lib_location = find_library(.opus.)/lib_location = "libopus.so.0"/' \
    /app/.venv/lib/python3.13/site-packages/opuslib/api/__init__.py

# Copy only runtime source from the host. Generated protocol artifacts are
# copied from protocol-builder so host artifacts cannot leak into the image.
COPY pyproject.toml uv.lock hatch_build.py ./
COPY config.yaml ./config.yaml
COPY src/ ./src/
COPY --from=protocol-builder /app/src/vector/_lib/ ./src/vector/_lib/
COPY --from=protocol-builder /app/src/vector/_protocol/ ./src/vector/_protocol/

# Install the copied project source into the final image. Use an explicit
# version when supplied, then any pre-generated version file, then a fallback.
# The protocol artifacts were copied from protocol-builder above, so skip the
# Hatchling protocol hook here to avoid rebuilding qlcp in the runtime stage.
ARG vector_VERSION
RUN --mount=type=cache,target=/root/.cache/uv \
    if [ -n "${vector_VERSION}" ]; then \
        version="${vector_VERSION}"; \
    elif [ -s src/vector/_version.py ]; then \
        version="$(python -c 'import runpy; print(runpy.run_path("src/vector/_version.py")["__version__"])')"; \
    else \
        version="0+unknown"; \
    fi && \
    SKIP_PROTOCOL_BUILD=1 SETUPTOOLS_SCM_PRETEND_VERSION="${version}" uv sync --locked --no-dev

RUN uv run --no-sync --no-dev python -c "from vector.qlcp.enums import PacketType; print(PacketType.ACK)"

CMD ["uv", "run", "--no-sync", "--no-dev", "python", "-m", "vector"]
