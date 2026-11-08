#!/usr/bin/env bash
set -euo pipefail
cd /workspace
export COLLECTOR_HOME=/tmp/tushare-offline-tests
python -m unittest discover -s 源码 -p 'test_*.py'
unset COLLECTOR_HOME
python -m PyInstaller --noconfirm --clean --onedir --name 采集器 --distpath /tmp/dist --workpath /tmp/work --specpath /tmp/spec --add-data '/workspace/源码/catalog.json:.' /workspace/源码/app.py
stage=/tmp/数据采集器_linux_Ubuntu24_x86_64
mkdir -p "$stage/Tushare数据采集器_linux" "$stage/源码" "$stage/scripts"
cp -a /tmp/dist/采集器/. "$stage/Tushare数据采集器_linux/"
cp README.md AGENTS.md requirements.txt requirements-build.txt .gitignore "$stage/"
cp 源码/*.py 源码/*.cjs 源码/catalog.json "$stage/源码/"
cp scripts/install.sh scripts/start.sh scripts/stop.sh scripts/configure.sh scripts/status.sh scripts/access.sh "$stage/scripts/"
if [[ "${COLLECTOR_PACKAGE_BASELINE:-1}" == 1 && -f 基线/本机采集基线.json ]]; then
    STAGE="$stage" python - <<'PY'
import json,hashlib,os,shutil
from pathlib import Path
from sys import path
path.insert(0,'源码')
from baseline import canonical
source=Path('基线').resolve();dest=Path(os.environ['STAGE'])/'基线'
value=json.loads((source/'本机采集基线.json').read_text(encoding='utf-8'));ident=value.pop('manifest_id')
assert hashlib.sha256(canonical(value)).hexdigest()==ident
dest.mkdir()
shutil.copy2(source/'本机采集基线.json',dest/'本机采集基线.json')
for entry in value['catalog_files']:
    f=(source/entry['relative_path']).resolve();assert source in f.parents
    assert hashlib.sha256(f.read_bytes()).hexdigest()==entry['sha256']
    target=dest/entry['relative_path'];target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(f,target)
PY
fi
chmod +x "$stage"/scripts/*.sh "$stage/Tushare数据采集器_linux/采集器"
"$stage/Tushare数据采集器_linux/采集器" --version
# No credentials, states, data or local browser profiles are copied into the archive.
tar -C /tmp -czf /workspace/数据采集器_linux_Ubuntu24_x86_64.tar.gz 数据采集器_linux_Ubuntu24_x86_64
python 源码/test_package.py /workspace/数据采集器_linux_Ubuntu24_x86_64.tar.gz
cd /workspace
sha256sum 数据采集器_linux_Ubuntu24_x86_64.tar.gz > 数据采集器_linux_Ubuntu24_x86_64.tar.gz.sha256
