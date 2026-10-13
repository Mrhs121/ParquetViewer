#!/usr/bin/env bash
set -e

# ==============================================================================
# package_dmg.sh - 制作 Parquet QuickView macOS DMG 安装镜像
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_PATH="/Applications/Parquet QuickView.app"
DIST_DIR="$SCRIPT_DIR/dist"
VERSION="1.0.0"
DMG_NAME="Parquet-QuickView-${VERSION}.dmg"
DMG_PATH="$DIST_DIR/$DMG_NAME"
STAGING_DIR="/tmp/ParquetViewer_dmg_staging_$$"

# 确保 App 已经构建存在
if [ ! -d "$APP_PATH" ]; then
    echo "⚠️ 未在 /Applications 找到已构建的 App，正在先执行构建..."
    bash "$SCRIPT_DIR/build.sh"
fi

echo "==> 正在准备 DMG 安装镜像..."
mkdir -p "$DIST_DIR"
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR"

# 1. 拷贝最新 App
echo "==> 复制应用包到打包暂存区..."
cp -R "$APP_PATH" "$STAGING_DIR/"

# 2. 创建 Applications 软链接（实现经典的一键拖动安装）
ln -s /Applications "$STAGING_DIR/Applications"

# 3. 使用 hdiutil 制作压缩只读 DMG 镜像
echo "==> 正在使用 hdiutil 制作 DMG 磁盘镜像..."
rm -f "$DMG_PATH"
hdiutil create -volname "Parquet QuickView" \
    -srcfolder "$STAGING_DIR" \
    -ov -format UDZO \
    "$DMG_PATH"

# 4. 清理暂存区
rm -rf "$STAGING_DIR"

# 5. 输出结果
DMG_SIZE=$(du -sh "$DMG_PATH" | awk '{print $1}')
echo "=================================================="
echo "🎉 DMG 打包成功！"
echo "📦 镜像文件: $DMG_PATH"
echo "📏 镜像大小: $DMG_SIZE"
echo "💡 可直接分享该 .dmg 文件给任何 Mac 用户安装使用！"
echo "=================================================="
