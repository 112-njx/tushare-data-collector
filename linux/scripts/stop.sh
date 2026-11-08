#!/usr/bin/env bash
set -euo pipefail
# SIGTERM keeps the persisted resume intention; stopping systemd does not launch new requests.
systemctl stop tushare-collector.service
echo '服务已停止，已提交断点保留。'
