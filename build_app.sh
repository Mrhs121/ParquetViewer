#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SRC_DIR="$SCRIPT_DIR/src"
APP_NAME="Parquet QuickView.app"
APP_DIR="/Applications/$APP_NAME"
BIN_DIR="/Users/huangsheng/.local/bin"

echo "==> 1. 编译 Swift 可执行程序..."
swiftc -O -target arm64-apple-macosx14.0 "$SRC_DIR/main.swift" -o /tmp/ParquetViewer_bin

echo "==> 2. 构建 macOS App Bundle 结构..."
rm -rf "$APP_DIR"
mkdir -p "$APP_DIR/Contents/MacOS"
mkdir -p "$APP_DIR/Contents/Resources"

cp /tmp/ParquetViewer_bin "$APP_DIR/Contents/MacOS/ParquetViewer"
chmod +x "$APP_DIR/Contents/MacOS/ParquetViewer"

cp "$SRC_DIR/Info.plist" "$APP_DIR/Contents/Info.plist"
echo "APPLPQVW" > "$APP_DIR/Contents/PkgInfo"

cp "$SRC_DIR/index.html" "$APP_DIR/Contents/Resources/index.html"
cp "$SRC_DIR/app.js" "$APP_DIR/Contents/Resources/app.js"
cp "$SRC_DIR/parquet_engine.py" "$APP_DIR/Contents/Resources/parquet_engine.py"
chmod +x "$APP_DIR/Contents/Resources/parquet_engine.py"

if [ -f "/tmp/AppIcon.icns" ]; then
    cp "/tmp/AppIcon.icns" "$APP_DIR/Contents/Resources/AppIcon.icns"
fi

echo "==> 3. 设置 macOS 文件系统属性..."
touch "$APP_DIR"

echo "==> 4. 刷新 macOS LaunchServices 注册表 (使 Finder 支持右键打开与双击关联)..."
/System/Library/Frameworks/CoreServices.framework/Frameworks/LaunchServices.framework/Support/lsregister -f "$APP_DIR" 2>/dev/null || true

echo "==> 5. 创建命令行快捷指令 (parquet-view & pview)..."
mkdir -p "$BIN_DIR"
cat << 'EOF' > "$BIN_DIR/parquet-view"
#!/bin/bash
if [ -z "$1" ]; then
    open -a "Parquet QuickView"
else
    # Convert to absolute path
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

echo "==> 编译构建并安装完成！"
echo "App 路径: $APP_DIR"
echo "命令行指令: $BIN_DIR/parquet-view (别名 pview)"
