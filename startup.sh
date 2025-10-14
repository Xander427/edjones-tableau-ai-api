#!/bin/bash
set -e

echo "=== Installing system dependencies ==="
apt-get update -y && apt-get install -y \
    apt-transport-https \
    curl \
    gnupg \
    unixodbc \
    unixodbc-dev

echo "=== Adding Microsoft package repo ==="
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list

echo "=== Removing any existing ODBC drivers ==="
apt-get remove -y msodbcsql18 || true
apt-get purge -y msodbcsql18 || true
odbcinst -u -d -n "ODBC Driver 18 for SQL Server" || true

echo "=== Installing ODBC Driver 17 for SQL Server ==="
apt-get update -y
ACCEPT_EULA=Y apt-get install -y msodbcsql17

echo "=== Verifying driver installation ==="
odbcinst -q -d

echo "=== Starting FastAPI app with Gunicorn ==="
exec gunicorn app:app --workers 1 --bind=0.0.0.0:8000 --timeout 600 -k uvicorn.workers.UvicornWorker
