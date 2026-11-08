#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
export COLLECTOR_HOME="$ROOT"
"$ROOT/Tushare数据采集器_linux/采集器" --url
