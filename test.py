import pyodbc
from azure.identity import DefaultAzureCredential

print("pyodbc version:", pyodbc.version)
print("Drivers:", pyodbc.drivers())
print("Testing connect...")

try:
    # Acquire token
    credential = DefaultAzureCredential()
    token = credential.get_token("https://database.windows.net/.default")
    print("Token acquired:", token.token[:20] + "...")

    # Encode token properly for ODBC
    access_token = bytes(token.token, "utf-16-le")

    # Connection string
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=azsqlserverejcampaignmanager.database.windows.net;"
        "DATABASE=devazsqldbejcampaignmanager;"
        "Encrypt=yes;TrustServerCertificate=no;"
    )

    # Connect using token
    conn = pyodbc.connect(conn_str, attrs_before={1256: access_token})
    print("✅ Connected successfully")

    cursor = conn.cursor()
    cursor.execute("SELECT TOP 1 name FROM sys.databases")
    print("Query result:", cursor.fetchone())

    conn.close()

except Exception as e:
    print("❌ Exception:", e)
