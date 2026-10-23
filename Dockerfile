FROM rust:1.94.1-slim-trixie

WORKDIR /app

ENV APP_ENV="production"

COPY . .
RUN cargo build --profile maxperf

CMD ["./target/maxperf/lend_worker_stellar"]
