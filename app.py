from pydantic import BaseModel
from fastapi import FastAPI
import pyodbc
from azure.identity import DefaultAzureCredential #pip install azure-identity
from azure.identity import AzureCliCredential
import struct
import platform

credential = AzureCliCredential()

app = FastAPI()

# --- Pydantic model for requests ---
class AskRequest(BaseModel):
    question: str

# --- Healthcheck endpoint ---
@app.get("/")
def healthcheck():
    return {"status": "ok"}

# --- Database connection helper ---
def get_db_connection():
    """
    Returns a pyodbc connection to Azure SQL Database.
    - Windows local: uses AzureCliCredential + token
    - Linux / Azure App Service: uses DefaultAzureCredential + token
    """
    server = "azsqlserverejcampaignmanager.database.windows.net"
    database = "devazsqldbejcampaignmanager"

    driver = "{ODBC Driver 17 for SQL Server}"
    encrypt = "yes"
    trust_cert = "no"

    conn_str = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
    )

    # --- Detect OS ---
    if platform.system() == "Windows":
        # Windows local development
        credential = AzureCliCredential()
        token = credential.get_token("https://database.windows.net/.default")

        # Token must be encoded for ODBC on Windows
        exptoken = b''
        for i in bytes(token.token, "utf-8"):
            exptoken += bytes([i])
            exptoken += b'\0'  # null byte after each char
        tokenstruct = struct.pack("=i", len(exptoken)) + exptoken

        conn = pyodbc.connect(conn_str, attrs_before={1256: tokenstruct})
        return conn

    else:
        # Linux / Azure App Service (managed identity)
        credential = DefaultAzureCredential()
        token = credential.get_token("https://database.windows.net/.default")
        token_bytes = bytes(token.token, "utf-8")

        conn = pyodbc.connect(conn_str, attrs_before={1256: token_bytes})
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
