@echo off
setlocal EnableExtensions

REM Ir para a pasta do projeto (onde está este .bat)
pushd "%~dp0"

REM Se não existir venv, cria
if not exist ".venv\Scripts\python.exe" (
    echo [.bat] .venv nao encontrada. Criando...
    python -m venv .venv
)

REM Garante que o pip esteja ok (opcional, mas recomendado)
REM ".venv\Scripts\python.exe" -m pip install --upgrade pip
REM ".venv\Scripts\python.exe" -m pip install -r requirements.txt

echo.
echo [.bat] Python em uso:
".venv\Scripts\python.exe" --version
echo.

REM Executa
".venv\Scripts\python.exe" main.py
set "EXITCODE=%ERRORLEVEL%"

popd

echo.
echo Execucao finalizada. ExitCode=%EXITCODE%
echo Tecle algo para sair.
pause >nul
exit /b %EXITCODE%
