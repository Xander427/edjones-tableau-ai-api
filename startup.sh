#!/bin/bash

# Exit on error
set -e

echo "=== Installing msodbcsql18 and unixodbc-dev ==="
apt-get update
apt-get install -y curl gnupg2 apt-transport-https

# Add Microsoft repo
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list

apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18 unixodbc-dev

echo "=== Starting FastAPI app with gunicorn ==="
gunicorn --bind=0.0.0.0:$PORT --timeout 600 -k uvicorn.workers.UvicornWorker app:app
