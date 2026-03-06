param(
  [Parameter(Mandatory = $true)]
  [string]$Version
)

$ErrorActionPreference = "Stop"

$RootDir = (Resolve-Path "$PSScriptRoot/../..").Path
$DistDir = Join-Path $RootDir "dist"
$BinName = "p2panda-file-sharing-gui"
$AssetsDir = Join-Path $RootDir "file-sharing/assets"
$Arch = if ($env:ARCH) { $env:ARCH } else { "x86_64" }

Set-Location $RootDir
if (Test-Path (Join-Path $DistDir "windows")) {
  Remove-Item (Join-Path $DistDir "windows") -Recurse -Force
}
New-Item -ItemType Directory -Force -Path (Join-Path $DistDir "windows") | Out-Null

cargo build --release -p $BinName

$MetaJson = cargo metadata --format-version 1 --no-deps 2>$null | ConvertFrom-Json
$TargetDir = if ($MetaJson -and $MetaJson.target_directory) { $MetaJson.target_directory } else { Join-Path $RootDir "target" }

$ExePath = Join-Path $TargetDir "release/$BinName.exe"
$ZipDir = Join-Path $DistDir "windows/p2panda-file-sharing-$Version-windows-$Arch"
New-Item -ItemType Directory -Force -Path $ZipDir | Out-Null
Copy-Item $ExePath (Join-Path $ZipDir "$BinName.exe")
Copy-Item (Join-Path $AssetsDir "icon.svg") (Join-Path $ZipDir "icon.svg")

@"
p2panda File Sharing $Version

Run:
  p2panda-file-sharing-gui.exe
"@ | Out-File -FilePath (Join-Path $ZipDir "README.txt") -Encoding utf8

$ZipPath = Join-Path $DistDir "p2panda-file-sharing-$Version-windows-$Arch.zip"
if (Test-Path $ZipPath) {
  Remove-Item $ZipPath -Force
}
Compress-Archive -Path (Join-Path $ZipDir "*") -DestinationPath $ZipPath

cargo wix --package $BinName
$MsiSource = Get-ChildItem -Path (Join-Path $TargetDir "wix") -Filter "*.msi" | Sort-Object LastWriteTime | Select-Object -Last 1
if (-not $MsiSource) {
  throw "failed to locate MSI output in target/wix"
}

$MsiPath = Join-Path $DistDir "p2panda-file-sharing-$Version-windows-$Arch.msi"
Copy-Item $MsiSource.FullName $MsiPath -Force

Write-Output $MsiPath
Write-Output $ZipPath
