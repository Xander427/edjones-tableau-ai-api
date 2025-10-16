import pyodbc
import struct
from azure.identity import DefaultAzureCredential

print("pyodbc version:", pyodbc.version)
print("Drivers:", pyodbc.drivers())
print("Testing connect...")

try:
    token = DefaultAzureCredential().get_token("https://database.windows.net/.default")
    print("Token acquired:", token.token[:20] + "...")

    # Encode UTF-16-LE and pack length prefix
    access_token = token.token.encode("utf-16-le")
    print("Access token length (bytes):", len(access_token))
    token_struct = struct.pack("=i", len(access_token)) + access_token
    print("Token struct length (bytes):", len(token_struct))

    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=azsqlserverejcampaignmanager.database.windows.net;"
        "DATABASE=devazsqldbejcampaignmanager;"
        "Encrypt=yes;TrustServerCertificate=no;"
    )

    conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
    print("✅ Connected successfully")
except Exception as e:
    print("❌ Exception:", e)
