param([string]$RepositoryRoot)
$ErrorActionPreference = 'Stop'
$sourceRoot = $PSScriptRoot
if (-not $RepositoryRoot) { $RepositoryRoot = Join-Path (Split-Path $sourceRoot -Parent) 'Tushare_dataapp' }
if (-not (Test-Path -LiteralPath (Join-Path $RepositoryRoot '.git'))) { throw '未找到指定Git仓库' }
$destination = Join-Path $RepositoryRoot 'linux'
New-Item -ItemType Directory -Path $destination -Force | Out-Null
$names = @('.gitignore','.dockerignore','AGENTS.md','README.md','requirements.txt','requirements-build.txt','打包.ps1','同步源码到仓库.ps1')
foreach ($name in $names) { Copy-Item -LiteralPath (Join-Path $sourceRoot $name) -Destination (Join-Path $destination $name) -Force }
foreach ($subdir in @('源码','scripts')) {
    $outDir = Join-Path $destination $subdir
    New-Item -ItemType Directory -Path $outDir -Force | Out-Null
    $files = Get-ChildItem -LiteralPath (Join-Path $sourceRoot $subdir) -File
    foreach ($file in $files) {
        $allowed = if ($subdir -eq '源码') { $file.Extension -in @('.py','.cjs') -or $file.Name -eq 'catalog.json' } else { $file.Extension -eq '.sh' -or $file.Name -eq 'Dockerfile.build' }
        if ($allowed) { Copy-Item -LiteralPath $file.FullName -Destination (Join-Path $outDir $file.Name) -Force }
    }
}
Write-Output "已同步允许的源码至 $destination；未复制基线、数据、状态、密钥或部署包。"
