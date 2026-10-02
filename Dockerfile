# syntax=docker/dockerfile:1

# ------------------------------------------------------------------------------
# Stage 1: Build release binary with full feature flags
# ------------------------------------------------------------------------------
FROM rust:slim-bookworm AS builder

WORKDIR /usr/src/flotilla

# Install required build dependencies (protobuf compiler, pkg-config, SSL)
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy manifest and source files
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY benches ./benches
COPY src ./src
COPY README.md LICENSE ./

# Build optimized release binary
RUN cargo build --release --bin flotilla-server --features full

# Strip binary to minimize final footprint
RUN strip /usr/src/flotilla/target/release/flotilla-server

# ------------------------------------------------------------------------------
# Stage 2: Minimal runtime image
# ------------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Install TLS certificates and curl for container health checks
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Run as non-privileged service user
RUN groupadd -g 10001 flotilla && \
    useradd -u 10001 -g flotilla -s /bin/false -m flotilla

WORKDIR /app

# Copy stripped binary from builder
COPY --from=builder /usr/src/flotilla/target/release/flotilla-server /usr/local/bin/flotilla-server

USER flotilla:flotilla

# Expose default protocol ports:
#   9000/udp  - UDP Raft consensus datagrams
#   9001/tcp  - TCP client proposal & cluster streams
#   50051/tcp - HTTP/2 gRPC services (Propose, Step, Status)
EXPOSE 9000/udp 9001/tcp 50051/tcp

ENTRYPOINT ["flotilla-server"]
CMD []
