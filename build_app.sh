#!/usr/bin/env bash
# 兼容旧调用入口，转发至 build.sh
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "$DIR/build.sh" "$@"
