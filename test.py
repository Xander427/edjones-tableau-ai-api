import pyodbc
from azure.identity import DefaultAzureCredential

print("pyodbc version:", pyodbc.version)
print("Drivers:", pyodbc.drivers())
print("Testing connect...")

try:
    token = DefaultAzureCredential().get_token("https://database.windows.net/.default")
    print("Token acquired:", token.token[:20] + "...")
    access_token = token.token.encode('utf-16-le')
    print("Access token (utf-16-le):", access_token[:20] + "...")
    conn_str = "DRIVER={ODBC Driver 18 for SQL Server};SERVER=yourserver.database.windows.net;DATABASE=yourdb;Encrypt=yes;TrustServerCertificate=no;"
    conn = pyodbc.connect(conn_str, attrs_before={1256: access_token})
    print("✅ Connected successfully")
except Exception as e:
    print("❌ Exception:", e)
