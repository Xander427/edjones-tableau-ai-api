from pydantic import BaseModel
from fastapi import FastAPI
import pyodbc
from azure.identity import DefaultAzureCredential #pip install azure-identity
from azure.identity import AzureCliCredential
import struct
import platform
pyodbc.pooling = False # Disable connection pooling for token-based auth TEST
import time
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")


credential = AzureCliCredential()

app = FastAPI()

# --- Pydantic model for requests ---
class AskRequest(BaseModel):
    question: str

# --- Healthcheck endpoint ---
@app.get("/")
def healthcheck():
    return {"status": "ok"}

def get_db_connection(retries=3, delay=3):
    """
    Returns a pyodbc connection to Azure SQL Database.
    - Windows local: uses AzureCliCredential + token
    - Linux / Azure App Service: uses DefaultAzureCredential + token
    """
    server = os.getenv("SQL_SERVER", "azsqlserverejcampaignmanager.database.windows.net")
    database = os.getenv("SQL_DATABASE", "devazsqldbejcampaignmanager")


    driver = "{ODBC Driver 17 for SQL Server}"
    encrypt = "yes"
    trust_cert = "no"

    conn_str = (
        f"Driver={driver};"
        f"Server={server};"
        f"Database={database};"
        f"Encrypt={encrypt};TrustServerCertificate={trust_cert};"
    )

    for attempt in range(1, retries + 1):
        try:
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
                logger.info("✅ Connected successfully (Windows)")
                return conn

            else:
                # Linux / Azure App Service (Managed Identity)
                credential = DefaultAzureCredential()
                token = credential.get_token("https://database.windows.net/.default")

                # Encode UTF-16-LE and pack length prefix
                access_token = token.token.encode("utf-16-le")
                # =i = little-endian 4-byte integer (matches ODBC driver expectation)
                # Without this length prefix, the driver reads memory incorrectly → segfault 139.
                token_struct = struct.pack("=i", len(access_token)) + access_token

                conn = pyodbc.connect(conn_str, attrs_before={1256: token_struct})
                logger.info("✅ Connected successfully (Linux/Azure)")
                return conn
        except Exception as e:
            logger.warning(f"⚠️ Connection attempt {attempt} failed: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                logger.error("❌ All connection attempts failed.")
                raise

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

# --- DB Test endpoint ---
@app.get("/db-test")
async def db_test():
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT TOP 1 question FROM tableau_ai_test ORDER BY id DESC;")
            row = cursor.fetchone()
            return {"status": "ok", "last_question": row[0] if row else None}
    except Exception as e:
        return {"status": "error", "message": str(e)}

