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

echo "=== [Startup] Adding Microsoft package repo ==="
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

echo "=== [Startup] Removing ODBC Driver 17 ==="
apt-get remove -y msodbcsql17 || true

echo "=== [Startup] Installing ODBC Driver 18 ==="
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
apt-get update -y
ACCEPT_EULA=Y apt-get install -y msodbcsql18


#echo "=== [Startup] Reinstall pyodbc 5.2.0 ==="
#pip install --no-cache-dir --force-reinstall pyodbc==5.2.0

echo "=== [Startup] Verify drivers ==="
odbcinst -q -d

echo "=== [Startup] Test pyodbc ==="
python3 - <<'EOF'
import pyodbc
print("pyodbc version:", pyodbc.version)
print("Drivers found:", pyodbc.drivers())
EOF

echo "=== [Startup] Launching FastAPI app ==="
exec gunicorn app:app --workers 1 --bind=0.0.0.0:8000 --timeout 600 -k uvicorn.workers.UvicornWorker
