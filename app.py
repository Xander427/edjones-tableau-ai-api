from pydantic import BaseModel
from fastapi import FastAPI
import pyodbc
from azure.identity import DefaultAzureCredential #pip install azure-identity
from azure.identity import AzureCliCredential
import struct
import platform
pyodbc.pooling = False # Disable connection pooling for token-based auth TEST

credential = AzureCliCredential()

app = FastAPI()

# --- Pydantic model for requests ---
class AskRequest(BaseModel):
    question: str

# --- Healthcheck endpoint ---
@app.get("/")
def healthcheck():
    return {"status": "ok"}

def get_db_connection():
    """
    Returns a pyodbc connection to Azure SQL Database.
    - Windows local: uses AzureCliCredential + token
    - Linux / Azure App Service: uses DefaultAzureCredential + token
    """
    server = "azsqlserverejcampaignmanager.database.windows.net"
    database = "devazsqldbejcampaignmanager"

    driver = "{ODBC Driver 18 for SQL Server}"
    encrypt = "yes"
    trust_cert = "no"

    conn_str = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
    )

    if platform.system() == "Windows":
        # Windows local development
        credential = AzureCliCredential()
        token = credential.get_token("https://database.windows.net/.default")

        exptoken = b""
        for i in bytes(token.token, "utf-8"):
            exptoken += bytes([i])
            exptoken += b"\0"
        tokenstruct = struct.pack("=i", len(exptoken)) + exptoken

        conn = pyodbc.connect(conn_str, attrs_before={1256: tokenstruct})
        return conn

    else:
        # Linux / Azure App Service (Managed Identity)
        credential = DefaultAzureCredential()
        token = credential.get_token("https://database.windows.net/.default")

        # Encode UTF-16-LE and pack length prefix
        access_token = token.token.encode("utf-16-le")
        token_struct = struct.pack("=i", len(access_token)) + access_token

        conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
        return conn

# --- Ask endpoint ---
@app.post("/ask")
async def ask(payload: AskRequest):
    # Optional: store/fetch question from DB
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO tableau_ai_test (question) VALUES (?)", payload.question)
            conn.commit()
    except Exception as e:
        return {"answer": f"You asked: {payload.question}", "db_error": str(e)}

    return {"answer": f"You asked: {payload.question}"}
