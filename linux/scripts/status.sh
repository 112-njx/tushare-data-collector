#!/usr/bin/env bash
set -euo pipefail
systemctl --no-pager --full status tushare-collector.service
journalctl -u tushare-collector.service -n 40 --no-pager
