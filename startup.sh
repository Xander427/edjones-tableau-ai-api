#!/bin/bash
set -e

echo "=== [Startup] Installing ODBC Driver 17 for SQL Server ==="

# Add Microsoft repository (idempotent)
if [ ! -f /etc/apt/sources.list.d/mssql-release.list ]; then
    echo "Adding Microsoft package repository..."
    curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /usr/share/keyrings/microsoft.gpg
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/12/prod stable main" > /etc/apt/sources.list.d/mssql-release.list
fi

# Install ODBC 17 and dependencies
apt-get update -y
ACCEPT_EULA=Y apt-get install -y \
    msodbcsql17 \
    unixodbc \
    unixodbc-dev

# Verify driver installation
echo "=== [Startup] Verifying installed ODBC drivers ==="
odbcinst -q -d || echo "Warning: No ODBC drivers found!"

# Start FastAPI app using Gunicorn + Uvicorn worker
echo "=== [Startup] Launching FastAPI app ==="
exec gunicorn app:app \
    --workers 1 \
    --bind=0.0.0.0:8000 \
    --timeout 600 \
    -k uvicorn.workers.UvicornWorker
