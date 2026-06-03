FROM rust:1.88-slim-bullseye

WORKDIR /app

ENV APP_ENV="production"

RUN apt-get update && \
    apt-get install -y pkg-config libssl-dev clang lld && \
    apt-get clean

COPY . .
RUN cargo build --profile maxperf

CMD ["./target/maxperf/lend_worker_stellar"]
