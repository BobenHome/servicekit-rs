# 构建阶段：使用 Rust 基础镜像编译（指定 slim 以减小体积）
FROM rust:1.91-slim-bookworm AS builder
WORKDIR /app

# 声明 Build Arguments
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY

#在 RUN cargo build 之前设置
ENV HTTP_PROXY=$HTTP_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
ENV NO_PROXY=$NO_PROXY

# 安装 C++ 编译器和构建工具, 供 clickhouse-rs-cityhash-sys 等 sys 包使用
RUN apt-get update && apt-get install -y build-essential

# 复制 Cargo 文件先构建依赖，优化缓存
COPY Cargo.toml Cargo.lock ./

# 1. 创建一个假的 src/main.rs，这样 Cargo 就不会报错
RUN mkdir src && echo "fn main(){}" > src/main.rs

# 2. 运行构建。
# 因为 src/main.rs 是空的，Cargo 会跳过编译它，但会编译 Cargo.lock 中的所有依赖项
RUN cargo build --release

# 3. 删掉假的 main.rs，为真正的源代码做准备
RUN rm src/main.rs

# 复制源代码、config 和 .sqlx
COPY src ./src
COPY config ./config
# 复制 sqlx offline 数据
COPY .sqlx ./.sqlx
# 设置 offline 环境变量
ENV SQLX_OFFLINE=true
COPY queries ./queries

# 再次运行构建。
# 这一次，所有依赖都已缓存，Cargo 只会编译你自己的 'servicekit' 代码
RUN cargo build --release

# 运行阶段：使用轻量 Debian 镜像，仅含二进制和 config
FROM debian:bookworm-slim

WORKDIR /app
# 复制编译后的二进制和 config
COPY --from=builder /app/target/release/servicekit /app/servicekit
COPY --from=builder /app/config /app/config
# 暴露端口（假设 app 监听 8084）
EXPOSE 8084
# 运行命令
CMD ["/app/servicekit"]