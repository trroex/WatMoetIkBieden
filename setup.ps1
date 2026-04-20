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
& .\.venv\Scripts\pip install -e ".[dev]" 2>$null
& .\.venv\Scripts\pip install -e .

Write-Host ""
Write-Host "Done! Run the tool with:" -ForegroundColor Green
Write-Host '  .\.venv\Scripts\fetch-address "Keizersgracht 123, 1015 CJ Amsterdam"'
Write-Host "Or via the scripts runner:"
Write-Host '  .\.venv\Scripts\python scripts\fetch_address.py "Keizersgracht 123, 1015 CJ Amsterdam"'
