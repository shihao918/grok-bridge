@echo off
cd /d "C:\Users\Dean\Code\GitHub\grok-bridge"
"C:\Users\Dean\Code\GitHub\grok-bridge\.venv\Scripts\python.exe" -u "C:\Users\Dean\Code\GitHub\grok-bridge\daemon.py" >> "C:\Users\Dean\Code\GitHub\grok-bridge\logs\daemon_boot.log" 2>&1
