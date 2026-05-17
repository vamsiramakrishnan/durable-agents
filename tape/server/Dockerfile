# Build the Tape server. Build context = the `tape/` directory (so ../proto is
# available to build.rs):  docker build -f server/Dockerfile -t tape-server .
FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /work
COPY proto ./proto
COPY server ./server
WORKDIR /work/server
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /work/server/target/release/tape-server /usr/local/bin/tape-server
EXPOSE 7878
ENTRYPOINT ["tape-server"]
