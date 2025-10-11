#!/bin/bash

# 确保脚本在出错时退出
set -e

# 获取 Cargo.toml 中的版本号
VERSION=${1:-$(grep '^version =' Cargo.toml | awk -F'"' '{print $2}')}
if [ -z "$VERSION" ]; then
    echo "错误：无法从 Cargo.toml 读取版本号"
    exit 1
fi

echo "正在构建 servicekit，版本：$VERSION"

# 执行 cargo build
cargo build --target x86_64-unknown-linux-gnu --release

# 验证二进制文件是否存在
BINARY_PATH="target/x86_64-unknown-linux-gnu/release/servicekit"
if [ ! -f "$BINARY_PATH" ]; then
    echo "错误：编译失败，未找到 $BINARY_PATH"
    exit 1
fi

# 添加可执行权限
chmod +x "$BINARY_PATH"
echo "已为 $BINARY_PATH 添加可执行权限"

# 创建临时目录用于打包
TEMP_DIR="tmp_package"
mkdir -p "$TEMP_DIR"
cp "$BINARY_PATH" "$TEMP_DIR/servicekit"
cp -r config "$TEMP_DIR/"

# 压缩为 tar.gz，文件名包含版本号
OUTPUT_FILE="servicekit-v$VERSION.tar.gz"
tar -czf "$OUTPUT_FILE" -C "$TEMP_DIR" servicekit config
echo "已生成 $OUTPUT_FILE"

# 清理临时目录
rm -rf "$TEMP_DIR"
echo "清理临时目录完成"

echo "打包完成！你可以上传 $OUTPUT_FILE 到服务器"
echo "服务器端解压命令示例：tar -xzf $OUTPUT_FILE -C /path/to/deployment"