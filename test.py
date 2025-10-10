import pyodbc
from azure.identity import AzureCliCredential

server = "azsqlserverejcampaignmanager.database.windows.net"
database = "devazsqldbejcampaignmanager"

credential = AzureCliCredential()
token = credential.get_token("https://database.windows.net/.default")
token_bytes = bytes(token.token, "utf-8")

conn_str = (
    f"Driver={{ODBC Driver 18 for SQL Server}};"
    f"Server={server};Database={database};"
    f"Authentication=ActiveDirectoryAccessToken;"
    f"Encrypt=yes;TrustServerCertificate=no;"
)

conn = pyodbc.connect(conn_str, attrs_before={1256: token_bytes})
cursor = conn.cursor()
cursor.execute("SELECT TOP 1 1")
print(cursor.fetchone())