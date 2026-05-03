# One-shot setup script for WatMoetIkBieden
# Run from the repo root in PowerShell:
#   .\setup.ps1
#
# Requirements: Python 3.11+ must be installed.
# Recommended installer: https://www.python.org/downloads/

$ErrorActionPreference = "Stop"

# Use 'py' launcher (Windows) or 'python3' (other)
$PY = if (Get-Command py -ErrorAction SilentlyContinue) { "py -3.11" } else { "python3" }

Write-Host "Creating virtual environment..." -ForegroundColor Cyan
Invoke-Expression "$PY -m venv .venv"

Write-Host "Activating venv and installing dependencies..." -ForegroundColor Cyan
& .\.venv\Scripts\pip install -e ".[ui]"

Write-Host ""
Write-Host "Done! Run the web interface with:" -ForegroundColor Green
Write-Host '  .\.venv\Scripts\streamlit run app.py'
Write-Host ""
Write-Host "Or use the CLI:" -ForegroundColor Green
Write-Host '  .\.venv\Scripts\fetch-address "Keizersgracht 123, 1015 CJ Amsterdam"'
