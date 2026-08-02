# kaas-consumer-canary, in two stages.
#
# Static musl on distroless, the same shape as kaas-producer-canary and kaas-ui
# and for the same reason: kaas-lib is pure Rust, and `rustls` with `ring`
# rather than `aws-lc-sys` means no cmake and no C project — just `musl-dev` for
# `ring` itself. Alpine is already musl-hosted, so there is no
# `rustup target add`.

# --- 1. build --------------------------------------------------------------
# The image tag is the toolchain pin for this build and must match
# rust-toolchain.toml. That file is deliberately not copied in: it asks for
# rust-src and rust-analyzer, which a release build has no use for and which
# rustup would spend a minute fetching into a discarded layer.
FROM rust:1.97.1-alpine AS build

RUN apk add --no-cache musl-dev

WORKDIR /src

# Manifests first, so the dependency layer survives a source-only change.
# kaas-lib's tree is not small and this is the difference between a 30-second
# rebuild and a five-minute one.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
 && echo 'fn main() {}' > src/main.rs \
 && cargo build --release \
 && rm -rf src

COPY src/ src/
# cargo caches on mtime, and the stub above may share a timestamp with the real
# source. Without this the stub binary is considered current and the image
# ships a program that does nothing at all, successfully — which for a monitor
# is the worst available outcome, since silence is what it reports with.
RUN touch src/main.rs && cargo build --release


# --- 2. runtime ------------------------------------------------------------
# `static-debian12` rather than `cc`: the binary is statically linked, so there
# is no libc to provide. No shell either, which is the point — there is nothing
# in this image to exec into.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /src/target/release/kaas-consumer-canary /kaas-consumer-canary

USER 65532:65532
ENTRYPOINT ["/kaas-consumer-canary"]
