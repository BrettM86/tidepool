# Lemmy built from source in DEBUG mode — the key trick that makes
# Lemmy↔Tidepool federation work over plain HTTP inside one compose network.
#
# Why a source build: whether Lemmy accepts http:// federation URLs is a
# COMPILE-TIME property, not configuration. Lemmy constructs its federation
# config with `.debug(cfg!(debug_assertions))` (src/lib.rs on 0.19.x;
# crates/server/src/lib.rs on main) and the activitypub-federation crate only
# sets `allow_http_urls` from that debug flag. Every published
# dessalines/lemmy image is a release build, which rejects http:// object IDs
# ("Http urls are only allowed in debug mode"), rejects explicit ports in
# URLs, and refuses private/local IPs — with NO runtime override for any of
# the three. A debug build accepts all three, which is exactly what
# LemmyNet's own docker/federation compose relies on (it builds from source
# with RUST_RELEASE_MODE=debug).
#
# The alternative (release image + caddy internal-CA TLS, portless https
# hostnames, custom CA trust inside the Lemmy container) was considered and
# rejected: Lemmy's reqwest TLS backend does not reliably honor a mounted
# CA, and the extra proxy indirection buys nothing for a local-only harness.
#
# Upstream's docker/Dockerfile cross-compiles arm64 via an amd64-hosted
# toolchain image (Rosetta emulation on Apple Silicon — very slow), so this
# Dockerfile builds natively for whatever platform docker runs on instead.
# First build is a full Rust compile (~15 min); the target-dir cache mount
# and the clone layer make rebuilds cheap.

FROM rust:1.81-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl-dev libpq-dev pkg-config git \
    && rm -rf /var/lib/apt/lists/*

# Pin the Lemmy release. lemmy-translations is a git submodule required to
# compile (crates/utils embeds it).
ARG LEMMY_VERSION=0.19.19
RUN git clone --depth 1 --branch ${LEMMY_VERSION} https://github.com/LemmyNet/lemmy.git /lemmy \
    && cd /lemmy \
    && git submodule update --init --recursive --depth 1

WORKDIR /lemmy

# Debug build (no --release) — see header comment. The cache mount keeps the
# cargo target dir across image rebuilds.
RUN --mount=type=cache,target=/lemmy/target,sharing=locked \
    cargo build \
    && cp target/debug/lemmy_server /usr/local/bin/lemmy_server

FROM debian:bookworm-slim

# Runtime deps mirror upstream's runner stage: libssl/libpq are linked by the
# binary, ca-certificates for any https egress, curl for compose healthchecks.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libssl3 libpq5 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local/bin/lemmy_server /usr/local/bin/lemmy_server

RUN useradd -m -u 1000 lemmy
USER lemmy

ENTRYPOINT ["lemmy_server"]
# lemmy.hjson binds port 80 (portless federation ids — see header comment).
EXPOSE 80
STOPSIGNAL SIGTERM
