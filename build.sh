#!/usr/bin/env bash
set -e

# ==============================================================================
# Parquet QuickView - 一键构建与打包脚本
# 支持自动检查并安装依赖、Universal 二进制编译 (Apple Silicon + Intel)、以及 DMG 镜像打包
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$SCRIPT_DIR/src"
ASSETS_DIR="$SCRIPT_DIR/assets"
APP_NAME="Parquet QuickView.app"
APP_DIR="/Applications/$APP_NAME"
BIN_DIR="${HOME}/.local/bin"
DIST_DIR="$SCRIPT_DIR/dist"
VERSION="1.0.0"

# 参数解析
BUILD_DMG=false
EMBED_DEPS=false

for arg in "$@"; do
    case $arg in
        --dmg)
            BUILD_DMG=true
            ;;
        --embed-deps)
            EMBED_DEPS=true
            ;;
        --help|-h)
            echo "用法: ./build.sh [选项]"
            echo "选项:"
            echo "  --dmg          构建完成后同时打包生成 .dmg 安装镜像 (输出至 dist/)"
            echo "  --embed-deps   将 Python 依赖库打包进 App Bundle (便于直接复制给其他人使用)"
            echo "  -h, --help     显示帮助信息"
            exit 0
            ;;
    esac
done

echo "=================================================="
echo "🚀 开始构建 Parquet QuickView v${VERSION}"
echo "=================================================="

# 1. 检查 Swift 编译器
if ! command -v swiftc >/dev/null 2>&1; then
    echo "❌ 错误: 未找到 swiftc 编译器。请先安装 Xcode Command Line Tools: xcode-select --install" >&2
    exit 1
fi

# 2. 检查 Python 环境与依赖
echo "==> 1. 检查 Python 运行依赖 (duckdb, pyarrow, fastavro)..."
if ! python3 -c "import duckdb, pyarrow, fastavro" 2>/dev/null; then
    echo "⚠️ 检测到缺少 Python 依赖库，正在通过 pip 自动安装..."
    if [ -f "$SCRIPT_DIR/requirements.txt" ]; then
        python3 -m pip install -r "$SCRIPT_DIR/requirements.txt" || \
        python3 -m pip install --user -r "$SCRIPT_DIR/requirements.txt"
    else
        python3 -m pip install duckdb pyarrow fastavro || \
        python3 -m pip install --user duckdb pyarrow fastavro
    fi
    echo "✅ Python 依赖库安装完成！"
else
    echo "✅ Python 依赖已满足。"
fi

# 3. 编译原生 Universal 2 二进制 (Apple Silicon + Intel)
echo "==> 2. 编译 Universal 2 二进制 (arm64 + x86_64)..."
BUILD_TEMP="/tmp/ParquetViewer_build_$$"
mkdir -p "$BUILD_TEMP"

swiftc -O -target arm64-apple-macosx12.0 "$SRC_DIR/main.swift" -o "$BUILD_TEMP/bin_arm64"
swiftc -O -target x86_64-apple-macosx12.0 "$SRC_DIR/main.swift" -o "$BUILD_TEMP/bin_x86"
lipo -create -output "$BUILD_TEMP/ParquetViewer" "$BUILD_TEMP/bin_arm64" "$BUILD_TEMP/bin_x86"
chmod +x "$BUILD_TEMP/ParquetViewer"

# 4. 构建 macOS App Bundle 目录结构
echo "==> 3. 生成 App Bundle 文件结构..."
rm -rf "$APP_DIR"
mkdir -p "$APP_DIR/Contents/MacOS"
mkdir -p "$APP_DIR/Contents/Resources/samples"

cp "$BUILD_TEMP/ParquetViewer" "$APP_DIR/Contents/MacOS/ParquetViewer"
cp "$SRC_DIR/Info.plist" "$APP_DIR/Contents/Info.plist"
echo "APPLPQVW" > "$APP_DIR/Contents/PkgInfo"

cp "$SRC_DIR/index.html" "$APP_DIR/Contents/Resources/index.html"
cp "$SRC_DIR/app.js" "$APP_DIR/Contents/Resources/app.js"
cp "$SRC_DIR/parquet_engine.py" "$APP_DIR/Contents/Resources/parquet_engine.py"
chmod +x "$APP_DIR/Contents/Resources/parquet_engine.py"

# 拷贝应用图标
if [ -f "$ASSETS_DIR/AppIcon.icns" ]; then
    cp "$ASSETS_DIR/AppIcon.icns" "$APP_DIR/Contents/Resources/AppIcon.icns"
elif [ -f "/tmp/AppIcon.icns" ]; then
    cp "/tmp/AppIcon.icns" "$APP_DIR/Contents/Resources/AppIcon.icns"
fi

# 拷贝内置示例数据
if [ -d "$ASSETS_DIR/samples" ]; then
    cp -R "$ASSETS_DIR/samples/"* "$APP_DIR/Contents/Resources/samples/"
fi

# 如果需要内嵌 Python 依赖库
if [ "$EMBED_DEPS" = true ]; then
    echo "==> 4. 内嵌 Python 运行依赖库到 App Bundle (用于便携分发)..."
    mkdir -p "$APP_DIR/Contents/Resources/python_lib"
    python3 -m pip install -t "$APP_DIR/Contents/Resources/python_lib" -r "$SCRIPT_DIR/requirements.txt" --no-compile
fi

# 清理临时编译目录
rm -rf "$BUILD_TEMP"

# 5. 刷新 macOS 系统文件属性与 LaunchServices 注册表
echo "==> 5. 刷新 macOS LaunchServices 注册表..."
touch "$APP_DIR"
/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f "$APP_DIR" 2>/dev/null || true

# 6. 配置全局命令行快捷指令
echo "==> 6. 设置命令行快捷指令 (parquet-view & pview)..."
mkdir -p "$BIN_DIR"
cat << 'EOF' > "$BIN_DIR/parquet-view"
#!/bin/bash
if [ -z "$1" ]; then
    open -a "Parquet QuickView"
else
    # 转换为绝对路径
    if [ -d "$1" ]; then
        TARGET_DIR="$(cd "$1" && pwd)"
        open -a "Parquet QuickView" "$TARGET_DIR"
    elif [ -f "$1" ]; then
        TARGET_FILE="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
        open -a "Parquet QuickView" "$TARGET_FILE"
    else
        echo "错误: 找不到文件或路径: $1" >&2
        exit 1
    fi
fi
EOF
chmod +x "$BIN_DIR/parquet-view"
ln -sf "$BIN_DIR/parquet-view" "$BIN_DIR/pview"

echo "✅ App 构建并安装成功！"
echo "   - 应用程序路径: $APP_DIR"
echo "   - 命令行快捷指令: $BIN_DIR/parquet-view (别名 pview)"

# 7. 打包 DMG 镜像 (如果指定了 --dmg)
if [ "$BUILD_DMG" = true ]; then
    echo ""
    echo "==> 7. 制作 DMG 安装镜像..."
    bash "$SCRIPT_DIR/package_dmg.sh"
fi
