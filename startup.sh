#!/bin/bash
set -e

echo "=== [Startup] Installing system dependencies ==="
apt-get update -y && apt-get install -y \
    apt-transport-https \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev \
    g++

echo "=== [Startup] Adding Microsoft package repo (Debian 12 / Bookworm) ==="
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor | tee /usr/share/keyrings/microsoft.gpg > /dev/null
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list

echo "=== [Startup] Removing ODBC Driver 17 (if exists) ==="
apt-get remove -y msodbcsql17 || true

echo "=== [Startup] Installing ODBC Driver 18 ==="
apt-get update -y
ACCEPT_EULA=Y apt-get install -y msodbcsql18

echo "=== [Startup] Verify ODBC drivers ==="
odbcinst -q -d

echo "=== [Startup] Testing pyodbc ==="
python3 - <<'EOF'
import pyodbc
print("pyodbc version:", pyodbc.version)
print("Drivers found:", pyodbc.drivers())
EOF

echo "=== [Startup] Launching FastAPI app ==="
exec gunicorn app:app --workers 1 --bind=0.0.0.0:8000 --timeout 600 -k uvicorn.workers.UvicornWorker
