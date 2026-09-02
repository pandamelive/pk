FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive \
    RUSTUP_HOME=/opt/rustup \
    CARGO_HOME=/opt/cargo \
    PATH=/opt/cargo/bin:/opt/runner:$PATH \
    SCCACHE_DIR=/opt/cargo/sccache \
    CARGO_INCREMENTAL=0

# 系统依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    pkg-config \
    libssl-dev \
    musl-tools \
    cmake \
    ninja-build \
    clang \
    lld \
    mold \
    ca-certificates \
    tzdata \
    jq \
    && rm -rf /var/lib/apt/lists/*

# Rust 工具链
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal \
    && rustup target add \
        x86_64-unknown-linux-gnu \
        x86_64-unknown-linux-musl \
        aarch64-unknown-linux-musl \
        wasm32-unknown-unknown \
    && rustup component add rustfmt clippy

# cargo 工具：sccache（编译缓存）、cross（交叉编译）、cargo-about
RUN cargo install sccache cross cargo-about --locked

# 配置 cargo 使用 sccache 和 mold 链接器
RUN mkdir -p /opt/cargo && printf '[build]\nrustc-wrapper = "/opt/cargo/bin/sccache"\n\n[target.x86_64-unknown-linux-gnu]\nlinker = "clang"\nrustflags = ["-C", "link-arg=-fuse-ld=mold"]\n' > /opt/cargo/config.toml

# GitHub Actions Runner
ARG RUNNER_VERSION=2.319.1
RUN arch=$(uname -m) && \
    curl -fsSL -o /tmp/runner.tar.gz \
    "https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/actions-runner-linux-${arch}-${RUNNER_VERSION}.tar.gz" && \
    mkdir -p /opt/runner && tar xzf /tmp/runner.tar.gz -C /opt/runner && \
    rm /tmp/runner.tar.gz && \
    /opt/runner/bin/installdependencies.sh

# 非 root 用户
RUN useradd -m -u 1000 runner && \
    chown -R runner:runner /opt/runner /opt/cargo /opt/rustup

USER runner
WORKDIR /home/runner

COPY entrypoint.sh /home/runner/entrypoint.sh
RUN chmod +x /home/runner/entrypoint.sh

ENTRYPOINT ["/home/runner/entrypoint.sh"]
