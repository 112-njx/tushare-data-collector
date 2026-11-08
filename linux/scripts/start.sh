#!/usr/bin/env bash
set -euo pipefail
systemctl start tushare-collector.service
systemctl --no-pager --full status tushare-collector.service
