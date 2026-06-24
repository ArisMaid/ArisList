FROM mcr.microsoft.com/devcontainers/typescript-node:1-22-bookworm AS web
USER root
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend ./
RUN npm run build

FROM mcr.microsoft.com/devcontainers/rust:1-bookworm AS server
USER root
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config libsqlite3-dev
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p media-shelf-server

FROM mcr.microsoft.com/devcontainers/base:bookworm
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libsqlite3-0
WORKDIR /app
COPY --from=server /app/target/release/media-shelf-server /usr/local/bin/media-shelf-server
COPY --from=web /app/frontend/dist /app/public
ENV STATIC_DIR=/app/public
EXPOSE 8787
CMD ["media-shelf-server"]
