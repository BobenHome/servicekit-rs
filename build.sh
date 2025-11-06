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
# 检测 macOS 并应用跨平台兼容修复（使用环境变量，避免 BSD tar 选项不支持）--owner=0 --group=0 选项是设置文件权限为root
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo "检测到 macOS，使用 no-xattrs disable-copyfile  禁用 Apple 元数据（零警告跨平台）"
    tar --no-xattrs --disable-copyfile --owner=0 --group=0 -czf "$OUTPUT_FILE" -C "$TEMP_DIR" servicekit config
else
    echo "非 macOS 环境，直接打包"
    tar --owner=0 --group=0 -czf "$OUTPUT_FILE" -C "$TEMP_DIR" servicekit config
fi

# 验证 tar 内容（业务安全检查）
if ! tar -tzf "$OUTPUT_FILE" | grep -q "servicekit"; then
    echo "错误：tar 内容验证失败（缺少 servicekit）"
    rm -rf "$OUTPUT_FILE"
    rm -rf "$TEMP_DIR"
    exit 1
fi
echo "已生成 $OUTPUT_FILE (大小: $(du -h "$OUTPUT_FILE" | cut -f1))"

# 清理临时目录
rm -rf "$TEMP_DIR"
echo "清理临时目录完成"

echo "打包完成！你可以上传 $OUTPUT_FILE 到服务器"
echo "服务器端解压命令示例：tar -xzf $OUTPUT_FILE -C /path/to/deployment"