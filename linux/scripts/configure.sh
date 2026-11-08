#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if systemctl is-active --quiet tushare-collector.service; then echo '请先 bash scripts/stop.sh 停止服务再配置'; exit 1; fi
export COLLECTOR_HOME="$ROOT" TZ=Asia/Shanghai
umask 077
exec "$ROOT/Tushare数据采集器_linux/采集器" --configure
