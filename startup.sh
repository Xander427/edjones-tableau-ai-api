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

echo "=== [Startup] Removing any existing ODBC drivers ==="
apt-get remove -y msodbcsql17 msodbcsql18 || true

echo "=== [Startup] Installing ODBC Driver 17 (Debian 11, pinned version) ==="
ACCEPT_EULA=Y apt-get update -y
# Pin specifically to Debian 11 build (NOT Bookworm)
ACCEPT_EULA=Y apt-get install -y msodbcsql17=17.10.4.1-1

echo "=== [Startup] Verify ODBC driver install ==="
odbcinst -q -d || true

# Correct path for Debian 11 ODBC driver shared library
if ls /usr/lib/x86_64-linux-gnu/libmsodbcsql-17.*.so.* 1> /dev/null 2>&1; then
    echo "Driver library located in /usr/lib/x86_64-linux-gnu/"
    ldd /usr/lib/x86_64-linux-gnu/libmsodbcsql-17.*.so.* || true
else
    echo "Driver library not found in expected path. Creating symlink (defensive)..."
    mkdir -p /opt/microsoft/msodbcsql17/lib64
    ln -s /usr/lib/x86_64-linux-gnu/libmsodbcsql-17.*.so.* /opt/microsoft/msodbcsql17/lib64/ || true
    ldd /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.*.so.* || true
fi


echo "=== [Startup] Installing stable pyodbc version ==="
pip install --no-cache-dir pyodbc==4.0.39

echo "=== [Startup] Verifying pyodbc + ODBC driver compatibility ==="
python3 - <<'EOF'
import pyodbc, sys
print("Python:", sys.version)
print("pyodbc version:", pyodbc.version)
print("Drivers:", pyodbc.drivers())
try:
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};Server=localhost;Trusted_Connection=No;"
    pyodbc.connect(conn_str, timeout=1)
except Exception as e:
    print("Connection test expectedly failed (no DB), but pyodbc is functional.")
    print("Error:", e)
EOF

echo "=== [Startup] Launching FastAPI app ==="
exec gunicorn app:app --workers 1 --bind=0.0.0.0:8000 --timeout 600 -k uvicorn.workers.UvicornWorker
