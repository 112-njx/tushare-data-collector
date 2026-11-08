param([string]$Python = 'python', [switch]$WithoutBaseline)
$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot
if (-not $WithoutBaseline) {
    $windowsRoot = Join-Path (Split-Path $root -Parent) 'Tushare_dataapp'
    & $Python (Join-Path $root '源码\export_baseline.py') --windows-root $windowsRoot --output (Join-Path $root '基线')
    if ($LASTEXITCODE -ne 0) { throw '本机采集基线导出失败；请先暂停Windows采集器' }
}
& docker build --platform linux/amd64 -t tushare-collector-builder:ubuntu24 -f (Join-Path $root 'scripts\Dockerfile.build') $root
if ($LASTEXITCODE -ne 0) { throw 'Ubuntu24构建环境准备失败' }
$includeBaseline = if ($WithoutBaseline) { '0' } else { '1' }
& docker run --rm --platform linux/amd64 --network none -e "COLLECTOR_PACKAGE_BASELINE=$includeBaseline" -v "${root}:/workspace" tushare-collector-builder:ubuntu24 bash /workspace/scripts/build_linux.sh
if ($LASTEXITCODE -ne 0) { throw '离线验证或Linux打包失败' }
Write-Output (Join-Path $root '数据采集器_linux_Ubuntu24_x86_64.tar.gz')
