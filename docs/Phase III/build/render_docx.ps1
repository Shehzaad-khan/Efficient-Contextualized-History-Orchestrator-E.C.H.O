param(
    [Parameter(Mandatory = $true)][string]$DocxPath,
    [Parameter(Mandatory = $true)][string]$PdfPath,
    [string]$OutlinePath = ""
)
# Export a .docx to PDF through the real Word (read-only, hidden window).
# With -OutlinePath, also write every numbered/heading paragraph as
# "<page>`t<list number>`t<style>`t<text>" so the manual table of contents can be checked.
$ErrorActionPreference = "Stop"

$app = New-Object -ComObject Word.Application
$app.Visible = $false
$app.DisplayAlerts = 0
$doc = $null
try {
    # Open(FileName, ConfirmConversions, ReadOnly, AddToRecentFiles)
    $doc = $app.Documents.Open($DocxPath, $false, $true, $false)
    $doc.Repaginate()
    # 17 = wdExportFormatPDF
    $doc.ExportAsFixedFormat($PdfPath, 17)
    Write-Output ("Exported {0} pages to {1}" -f $doc.ComputeStatistics(2), $PdfPath)
    if ($OutlinePath -ne "") {
        $lines = New-Object System.Collections.Generic.List[string]
        foreach ($p in $doc.Paragraphs) {
            $style = $p.Style.NameLocal
            $listString = $p.Range.ListFormat.ListString
            $isHeading = $style -like "Heading*" -or $style -eq "Title"
            if ($isHeading -or ($listString -ne $null -and $listString -ne "" -and $p.Range.ListFormat.ListType -ne 2)) {
                $text = $p.Range.Text.Trim()
                if ($text.Length -gt 80) { $text = $text.Substring(0, 80) }
                # 1 = wdActiveEndAdjustedPageNumber (respects the footer's page numbering)
                $page = $p.Range.Information(1)
                $lines.Add(("{0}`t{1}`t{2}`t{3}" -f $page, $listString, $style, $text))
            }
        }
        [System.IO.File]::WriteAllLines($OutlinePath, $lines, (New-Object System.Text.UTF8Encoding($false)))
        Write-Output ("Wrote outline to {0}" -f $OutlinePath)
    }
}
finally {
    if ($doc -ne $null) { $doc.Close([ref]0) }
    if ($app.Documents.Count -eq 0) { $app.Quit() }
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($app) | Out-Null
}
