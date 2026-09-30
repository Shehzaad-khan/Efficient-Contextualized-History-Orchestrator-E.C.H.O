param(
    [Parameter(Mandatory = $true)][string]$PptxPath,
    [Parameter(Mandatory = $true)][string]$OutDir,
    [int]$Width = 1280
)
# Export every slide of a deck to PNG through the real PowerPoint (read-only, hidden window).
$ErrorActionPreference = "Stop"
New-Item -ItemType Directory -Force $OutDir | Out-Null
Get-ChildItem $OutDir -Filter "slide-*.png" | Remove-Item -Force

$app = New-Object -ComObject PowerPoint.Application
$pres = $null
try {
    # Open(FileName, ReadOnly, Untitled, WithWindow)
    $pres = $app.Presentations.Open($PptxPath, -1, 0, 0)
    $height = [int]($Width * $pres.PageSetup.SlideHeight / $pres.PageSetup.SlideWidth)
    foreach ($slide in $pres.Slides) {
        $name = "slide-{0:D2}.png" -f $slide.SlideIndex
        $slide.Export((Join-Path $OutDir $name), "PNG", $Width, $height)
    }
    Write-Output ("Exported {0} slides to {1}" -f $pres.Slides.Count, $OutDir)
}
finally {
    if ($pres -ne $null) { $pres.Close() }
    if ($app.Presentations.Count -eq 0) { $app.Quit() }
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($app) | Out-Null
}
