@echo off
setlocal

REM 1) Run from this script's folder
cd /d "C:\Users\kittojor\OneDrive - Queensland Health\A001_Data_Analytics\A0017_PIOR" || (
  echo *** Failed to cd into script folder ***
  pause
  exit /b 1
)

REM 2) Prepare logs folder and timestamped logfile
if not exist logs mkdir logs
set "LOGFILE=logs\export_%DATE:~10,4%-%DATE:~7,2%-%DATE:~4,2%_%TIME:~0,2%-%TIME:~3,2%.log"
set "LOGFILE=%LOGFILE::=-%"

REM 3) Ensure unbuffered Python so prints flush immediately
set "PYTHONUNBUFFERED=1"

REM 4) Launch python via PowerShell without piping, preserving carriage-return refresh,
REM    while duplicating output to both console and logfile.
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
"$psi = New-Object System.Diagnostics.ProcessStartInfo; $psi.FileName = 'python'; $psi.Arguments = '-u main.py'; $psi.UseShellExecute = $false; $psi.RedirectStandardOutput = $true; $psi.RedirectStandardError = $true; $p = [System.Diagnostics.Process]::Start($psi); $fs = [System.IO.File]::Open('%LOGFILE%', [System.IO.FileMode]::Append, [System.IO.FileAccess]::Write, [System.IO.FileShare]::ReadWrite); $sw = New-Object System.IO.StreamWriter($fs); function Pump([System.IO.StreamReader] $r) { $buf = New-Object byte[] 4096; while (($n = $r.BaseStream.Read($buf,0,$buf.Length)) -gt 0) { $txt = [System.Text.Encoding]::UTF8.GetString($buf,0,$n); [Console]::Write($txt); $sw.Write($txt); $sw.Flush() } }; Pump $p.StandardOutput; Pump $p.StandardError; $p.WaitForExit(); $code = $p.ExitCode; $sw.Close(); $fs.Close(); exit $code"

set "EXITCODE=%ERRORLEVEL%"

if %EXITCODE% NEQ 0 (
  echo *** Script encountered an error; see "%LOGFILE%" for details ***
) else (
  echo Script completed successfully
)

echo.
pause
endlocal & exit /b %EXITCODE%
