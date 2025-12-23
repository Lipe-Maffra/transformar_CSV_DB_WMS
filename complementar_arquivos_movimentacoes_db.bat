@echo off
setlocal EnableExtensions

set "ROOT=C:\Users\felipe.maffra\Desktop\Python\Movimentacoes_DB"
set "SRC=%ROOT%\src"

REM garante pastas
mkdir "%ROOT%" 2>nul
mkdir "%SRC%" 2>nul

REM garante main.py dentro de src (em branco)
if not exist "%SRC%\main.py" type nul > "%SRC%\main.py"

REM cria .gitignore se não existir
if not exist "%ROOT%\.gitignore" (
  > "%ROOT%\.gitignore" (
    echo # Python
    echo __pycache__/
    echo *.py[cod]
    echo *.pyd
    echo *.so
    echo .Python
    echo.
    echo # Virtual env
    echo .venv/
    echo venv/
    echo ENV/
    echo env/
    echo.
    echo # IDEs
    echo .vscode/
    echo .idea/
    echo.
    echo # Logs / temp
    echo *.log
    echo *.tmp
    echo.
    echo # Data / DB
    echo *.db
    echo *.sqlite
    echo *.sqlite3
    echo data/
    echo.
    echo # OS
    echo Thumbs.db
    echo Desktop.ini
  )
)

REM opcional: README.md em branco
if not exist "%ROOT%\README.md" type nul > "%ROOT%\README.md"

echo.
echo OK. Complementos criados/garantidos:
echo - %SRC%\main.py
echo - %ROOT%\.gitignore
echo - %ROOT%\README.md
echo.

dir "%ROOT%" /b
echo.
dir "%SRC%" /b

pause
