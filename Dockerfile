FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake libssl-dev pkg-config zlib1g-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release -p kafkaopsdb

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 zlib1g && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/kafkaopsdb /usr/local/bin/kafkaopsdb
EXPOSE 8080
ENTRYPOINT ["kafkaopsdb"]
CMD ["standalone", "--config", "/etc/kafkaopsdb/kafkaopsdb.toml"]
