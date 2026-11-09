# syntax=docker/dockerfile:1

# The MSRV, so the image is built with what Cargo.toml promises, on the same Debian release as
# the runtime image, so the binary links against the glibc it runs with.
FROM rust:1.96-slim-trixie AS build
# Writes the license texts of the crates compiled into the binary, which their licenses require
# to travel with it. Installed before the sources are copied, so the layer outlives them.
RUN cargo install cargo-about --version 0.9.2 --locked --features cli
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY docker/about.toml docker/about.hbs ./docker/
# /data and the log directory are created here because the runtime image has no shell to create
# them.
RUN cargo build --release --locked --bin s3dal \
    && cargo about generate --locked -c docker/about.toml -o THIRD-PARTY-LICENSES docker/about.hbs \
    && mkdir /data /log

# distroless/cc carries glibc, libgcc and CA certificates (for HTTPS upstreams), and no shell
# or package manager. The nonroot tag runs as uid 65532.
FROM gcr.io/distroless/cc-debian13:nonroot
COPY --from=build /src/target/release/s3dal /usr/local/bin/s3dal
# The license and NOTICE travel with the binary, as the Apache License requires.
COPY LICENSE NOTICE /usr/share/doc/s3dal/
COPY --from=build /src/THIRD-PARTY-LICENSES /usr/share/doc/s3dal/
# A named volume copies this directory's owner when first mounted, so s3dal can write to it.
COPY --from=build --chown=65532:65532 /data /data
# For S3DAL_LOG_FILE: s3dal creates a log file's missing directories, but /var/log is root's.
# Not a volume: the log goes to stderr unless a file is asked for.
COPY --from=build --chown=65532:65532 /log /var/log/s3dal
# Keeps objects out of the container's writable layer, which is removed with the container,
# even when no volume is given.
VOLUME ["/data"]
# Defaults in a config file, the lowest precedence s3dal has: as flags or variables they would
# override a config file mounted by the user, while in a file `-e S3DAL_BACKEND=s3` still
# overrides them without replacing the command. Credentials are deliberately absent; s3dal
# refuses to start without a key pair. Owned by s3dal's user so the console can save edits to it;
# a named volume on /etc/s3dal starts as a copy of it with that owner, and keeps the edits.
COPY --chown=65532:65532 docker/s3dal.toml /etc/s3dal/s3dal.toml
ENV S3DAL_CONFIG=/etc/s3dal/s3dal.toml
# The S3 API, and the web console when S3DAL_CONSOLE_ADDRESS turns it on.
EXPOSE 9000 9001
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/s3dal"]
CMD ["serve"]
