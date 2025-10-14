#!/bin/bash
set -e

echo "=== [Startup] Installing dependencies ==="
apt-get update -y && apt-get install -y \
    apt-transport-https \
    curl \
    gnupg \
    unixodbc=2.3.11-2+deb12u1 \
    unixodbc-dev=2.3.11-2+deb12u1

echo "=== [Startup] Adding Microsoft package repo ==="
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

echo "=== [Startup] Removing ODBC Driver 18 entries ==="
apt-get remove -y msodbcsql18 || true
apt-get purge -y msodbcsql18 || true
odbcinst -u -d -n "ODBC Driver 18 for SQL Server" || true

echo "=== [Startup] Installing ODBC Driver 17 (stable) ==="
apt-get update -y
ACCEPT_EULA=Y apt-get install -y msodbcsql17=17.10.6.1-1

echo "=== [Startup] Re-installing compatible pyodbc ==="
pip install --no-cache-dir --force-reinstall pyodbc==4.0.39

echo "=== [Startup] Verifying driver installation ==="
odbcinst -q -d

echo "=== [Startup] Testing pyodbc driver ==="
python3 - <<'EOF'
import pyodbc
print("pyodbc version:", pyodbc.version)
print("Drivers found:", pyodbc.drivers())
EOF

echo "=== [Startup] Launching FastAPI app ==="
exec gunicorn app:app --workers 1 --bind=0.0.0.0:8000 --timeout 600 -k uvicorn.workers.UvicornWorker
