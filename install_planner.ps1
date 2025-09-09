# PowerShell Installation script for Enhanced Planner Agent
# This script installs all required dependencies for the planner agent

Write-Host "=== Enhanced Planner Agent Installation ===" -ForegroundColor Cyan
Write-Host ""

# Check if Python is installed
try {
    $pythonVersion = python --version 2>&1
    Write-Host "✅ Python version: $pythonVersion" -ForegroundColor Green
} catch {
    Write-Host "❌ Python is not installed. Please install Python 3.8+ first." -ForegroundColor Red
    exit 1
}

# Check if pip is installed
try {
    $pipVersion = pip --version 2>&1
    Write-Host "✅ pip is available" -ForegroundColor Green
} catch {
    Write-Host "❌ pip is not installed. Please install pip first." -ForegroundColor Red
    exit 1
}

Write-Host ""

# Create virtual environment (optional but recommended)
$createVenv = Read-Host "Do you want to create a virtual environment? (y/n)"
if ($createVenv -eq "y" -or $createVenv -eq "Y") {
    Write-Host "📦 Creating virtual environment..." -ForegroundColor Yellow
    python -m venv planner_env
    
    # Activate virtual environment
    & ".\planner_env\Scripts\Activate.ps1"
    
    Write-Host "✅ Virtual environment created and activated" -ForegroundColor Green
}

Write-Host ""
Write-Host "📥 Installing core dependencies..." -ForegroundColor Yellow

# Upgrade pip first
pip install --upgrade pip

# Install core requirements
Write-Host "Installing main requirements..." -ForegroundColor Yellow
pip install -r requirements.txt

Write-Host ""
Write-Host "📥 Installing planner-specific dependencies..." -ForegroundColor Yellow

# Install planner-specific requirements
pip install -r requirements-planner.txt

Write-Host ""
Write-Host "🔧 Installing optional dependencies..." -ForegroundColor Yellow

# Ask about optional dependencies
$installSpacy = Read-Host "Install spaCy for enhanced text processing? (y/n)"
if ($installSpacy -eq "y" -or $installSpacy -eq "Y") {
    pip install "spacy>=3.7.0"
    python -m spacy download en_core_web_sm
    Write-Host "✅ spaCy installed with English model" -ForegroundColor Green
}

$installNltk = Read-Host "Install NLTK for natural language processing? (y/n)"
if ($installNltk -eq "y" -or $installNltk -eq "Y") {
    pip install "nltk>=3.8.0"
    python -c "import nltk; nltk.download('punkt'); nltk.download('stopwords'); nltk.download('wordnet')"
    Write-Host "✅ NLTK installed with common datasets" -ForegroundColor Green
}

Write-Host ""
Write-Host "🎯 Verifying installation..." -ForegroundColor Yellow

# Test critical imports
$verificationScript = @"
import sys
import importlib

critical_packages = [
    'openai',
    'azure.storage.blob',
    'boxsdk',
    'dotenv',
    'pandas',
    'numpy',
    'aiohttp',
    'requests'
]

failed_imports = []

for package in critical_packages:
    try:
        importlib.import_module(package)
        print(f'✅ {package}')
    except ImportError as e:
        print(f'❌ {package}: {e}')
        failed_imports.append(package)

if failed_imports:
    print(f'\n❌ Failed to import: {failed_imports}')
    sys.exit(1)
else:
    print('\n🎉 All critical packages imported successfully!')
"@

$result = python -c $verificationScript

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "🎉 Installation completed successfully!" -ForegroundColor Green
    Write-Host ""
    Write-Host "📋 Next steps:" -ForegroundColor Cyan
    Write-Host "1. Copy .env.sample to .env.dev" -ForegroundColor White
    Write-Host "2. Update .env.dev with your credentials" -ForegroundColor White
    Write-Host "3. Run the planner agent: python agents/planner_agent.py" -ForegroundColor White
    Write-Host ""
    Write-Host "💡 Example configuration:" -ForegroundColor Cyan
    Write-Host "   Copy-Item .env.sample .env.dev" -ForegroundColor White
    Write-Host "   # Edit .env.dev with your Azure, Box, and Confluence credentials" -ForegroundColor White
    Write-Host "   python agents/planner_agent.py" -ForegroundColor White
    Write-Host ""
    
    # Additional Windows-specific instructions
    Write-Host "🪟 Windows-specific notes:" -ForegroundColor Cyan
    Write-Host "- Use PowerShell or Command Prompt to run the scripts" -ForegroundColor White
    Write-Host "- Ensure your antivirus allows Python script execution" -ForegroundColor White
    Write-Host "- If you encounter SSL issues, try: pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org <package>" -ForegroundColor White
    
} else {
    Write-Host ""
    Write-Host "❌ Installation failed. Please check the error messages above." -ForegroundColor Red
    exit 1
}
