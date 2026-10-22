# Forge: one static binary on a distroless base.
#
#   docker build -t forge .
#   docker run -p 4317:4317 -p 4318:4318 -p 8080:8080 -p 9090:9090 -v forge-data:/var/lib/forge forge
FROM rust:1.97-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends musl-tools && rm -rf /var/lib/apt/lists/*
ARG TARGET=x86_64-unknown-linux-musl
RUN rustup target add ${TARGET}
WORKDIR /src
COPY . .
# vortex-io builds one C++ file that only uses C headers; musl-gcc compiles it.
ENV CC_x86_64_unknown_linux_musl=musl-gcc \
    CXX_x86_64_unknown_linux_musl=musl-gcc \
    CC_aarch64_unknown_linux_musl=musl-gcc \
    CXX_aarch64_unknown_linux_musl=musl-gcc
RUN cargo build --release --locked --target ${TARGET} -p forge-cli \
 && cp target/${TARGET}/release/forge /forge

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /forge /usr/local/bin/forge
COPY forge.toml /etc/forge/forge.toml
ENV FORGE_CONFIG=/etc/forge/forge.toml
VOLUME ["/var/lib/forge"]
EXPOSE 4317 4318 8080 9090
USER nonroot
ENTRYPOINT ["/usr/local/bin/forge"]
CMD ["serve"]
