#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ $EUID != 0 ]]; then echo '请使用 sudo bash scripts/install.sh 安装系统服务'; exit 1; fi
if [[ "$(uname -m)" != x86_64 ]]; then echo '本包仅支持 Ubuntu 24.04 x86_64'; exit 1; fi
source /etc/os-release
if [[ "$ID" != ubuntu || "$VERSION_ID" != 24.04 ]]; then echo '此部署包按 Ubuntu 24.04 构建，请在匹配系统使用'; exit 1; fi
if [[ "$ROOT" == *%* || "$ROOT" == *$'\n'* ]]; then echo '部署目录不能包含百分号或换行'; exit 1; fi
BIN="$ROOT/Tushare数据采集器_linux/采集器"
test -x "$BIN" || { echo '未找到可执行程序，请完整解压部署包'; exit 1; }
install -d -m 700 "$ROOT/状态" "$ROOT/mydb"
cat > /etc/systemd/system/tushare-collector.service <<EOF
[Unit]
Description=Tushare Ubuntu continuous data collector
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=simple
WorkingDirectory="$ROOT"
ExecStart="$BIN" --no-browser
Environment="COLLECTOR_HOME=$ROOT"
Environment=COLLECTOR_PORT=18794
Environment=TZ=Asia/Shanghai
UMask=0077
Restart=on-failure
RestartSec=30
TimeoutStopSec=120
KillSignal=SIGTERM
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=read-only
ProtectSystem=strict
ReadWritePaths="$ROOT/状态" "$ROOT/mydb"

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable tushare-collector.service
echo '服务已安装且开机启用，尚未启动。下一步：bash scripts/configure.sh，然后 bash scripts/start.sh。'
