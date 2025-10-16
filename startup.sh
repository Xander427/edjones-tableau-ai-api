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

echo "=== [Startup] Adding Microsoft package repo (Debian 11 / Bullseye) ==="
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

echo "=== [Startup] Removing any existing ODBC driver ==="
apt-get remove -y msodbcsql17 msodbcsql18 || true

echo "=== [Startup] Installing ODBC Driver 18 (Debian 11 build) ==="
ACCEPT_EULA=Y apt-get update -y
ACCEPT_EULA=Y apt-get install -y msodbcsql18

echo "=== [Startup] Verify ODBC driver ==="
odbcinst -q -d

echo "=== [Startup] Test pyodbc ==="
python3 - <<'EOF'
import pyodbc
print("pyodbc version:", pyodbc.version)
print("Drivers:", pyodbc.drivers())
EOF

echo "=== [Startup] Launching FastAPI app ==="
exec gunicorn app:app --workers 1 --bind=0.0.0.0:8000 --timeout 600 -k uvicorn.workers.UvicornWorker
