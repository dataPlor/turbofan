FROM --platform=linux/arm64 rust:1-slim-bookworm AS builder

COPY --from=proxy-ca proxy-ca.crt /usr/local/share/ca-certificates/proxy-ca.crt
RUN apt-get update && apt-get install -y pkg-config libssl-dev ca-certificates && update-ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Cargo.toml .
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src

COPY src src
RUN touch src/main.rs && cargo build --release

FROM --platform=linux/arm64 debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/hello-rust /usr/local/bin/hello-rust
CMD ["hello-rust"]
