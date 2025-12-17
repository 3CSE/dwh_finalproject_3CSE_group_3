# Export Metabase Dashboards to dashboard folder
# This script uses Metabase API to export all dashboards as JSON

$METABASE_URL = "http://localhost:3000"
$EXPORT_DIR = ".\dashboard"

# Create export directory if it doesn't exist
New-Item -ItemType Directory -Force -Path $EXPORT_DIR | Out-Null

Write-Host "Metabase Dashboard Export Script" -ForegroundColor Cyan
Write-Host "=================================" -ForegroundColor Cyan
Write-Host ""

# Get credentials
Write-Host "Enter Metabase credentials:" -ForegroundColor Yellow
$email = Read-Host "Email"
$password = Read-Host "Password" -AsSecureString
$passwordPlain = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($password))

# Login to get session token
Write-Host "`nLogging in to Metabase..." -ForegroundColor Yellow
try {
    $loginBody = @{
        username = $email
        password = $passwordPlain
    } | ConvertTo-Json

    $session = Invoke-RestMethod -Uri "$METABASE_URL/api/session" `
        -Method Post `
        -ContentType "application/json" `
        -Body $loginBody

    $sessionToken = $session.id
    Write-Host "✓ Login successful!" -ForegroundColor Green
} catch {
    Write-Host "✗ Login failed: $_" -ForegroundColor Red
    exit 1
}

# Get all dashboards
Write-Host "`nFetching dashboards..." -ForegroundColor Yellow
try {
    $headers = @{
        "X-Metabase-Session" = $sessionToken
    }

    $dashboards = Invoke-RestMethod -Uri "$METABASE_URL/api/dashboard" `
        -Method Get `
        -Headers $headers

    Write-Host "✓ Found $($dashboards.Count) dashboard(s)" -ForegroundColor Green
} catch {
    Write-Host "✗ Failed to fetch dashboards: $_" -ForegroundColor Red
    exit 1
}

# Export each dashboard
Write-Host "`nExporting dashboards..." -ForegroundColor Yellow
foreach ($dashboard in $dashboards) {
    try {
        $dashboardId = $dashboard.id
        $dashboardName = $dashboard.name
        $fileName = $dashboardName -replace '[^\w\s-]', '' -replace '\s+', '_'
        $fileName = $fileName.ToLower()
        
        Write-Host "  - Exporting: $dashboardName" -ForegroundColor Cyan
        
        # Get full dashboard details
        $fullDashboard = Invoke-RestMethod -Uri "$METABASE_URL/api/dashboard/$dashboardId" `
            -Method Get `
            -Headers $headers

        # Save to JSON file directly in dashboard folder
        $outputPath = Join-Path $EXPORT_DIR "$fileName.json"
        $fullDashboard | ConvertTo-Json -Depth 10 | Out-File -FilePath $outputPath -Encoding UTF8
        
        Write-Host "    ✓ Saved to: $outputPath" -ForegroundColor Green
    } catch {
        Write-Host "    ✗ Failed to export $dashboardName : $_" -ForegroundColor Red
    }
}

Write-Host "`n=================================" -ForegroundColor Cyan
Write-Host "Export complete!" -ForegroundColor Green
Write-Host "`nExported files are in: $EXPORT_DIR" -ForegroundColor Yellow
Write-Host "`nNext steps:" -ForegroundColor Yellow
Write-Host "  1. Review the exported JSON files in dashboard/"
Write-Host "  2. git add dashboard/*.json"
Write-Host "  3. git commit -m 'Export Metabase dashboards'"
Write-Host "  4. git push"
