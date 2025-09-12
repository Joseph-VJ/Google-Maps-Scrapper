@echo off
echo Starting Google Maps Scraper Web Interface...
echo.
echo The web interface will be available at:
echo http://127.0.0.1:5000
echo.
echo Press Ctrl+C to stop the server
echo.
cd /d "%~dp0"
C:/Python313/python.exe app.py
pause
