param([string]$Python = 'python')
$ErrorActionPreference = 'Stop'
$appRoot = $PSScriptRoot
$buildPython = Join-Path $appRoot '构建环境\Scripts\python.exe'
$env:TEMP = Join-Path $appRoot '构建临时'
$env:TMP = $env:TEMP
$env:PYINSTALLER_CONFIG_DIR = Join-Path $appRoot '构建临时\缓存'
$env:PIP_CACHE_DIR = Join-Path $appRoot '构建临时\pip缓存'
New-Item -ItemType Directory -Path $env:TEMP -Force | Out-Null
if (-not (Test-Path -LiteralPath $buildPython)) {
    & $Python -m venv (Join-Path $appRoot '构建环境')
    if ($LASTEXITCODE -ne 0) { throw '创建本项目构建环境失败' }
}
& $buildPython -m pip install -r (Join-Path $appRoot 'requirements-build.txt')
if ($LASTEXITCODE -ne 0) { throw '安装构建依赖失败' }
& $buildPython -m PyInstaller --noconfirm --clean --onedir --windowed --name 'Tushare数据采集器' --distpath $appRoot --workpath (Join-Path $appRoot '构建临时\PyInstaller') --specpath (Join-Path $appRoot '源码') --add-data "$(Join-Path $appRoot '源码\catalog.json');." (Join-Path $appRoot '源码\app.py')
if ($LASTEXITCODE -ne 0) { throw '构建失败' }
