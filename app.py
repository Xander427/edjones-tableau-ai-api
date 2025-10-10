from pydantic import BaseModel
from fastapi import FastAPI
import pyodbc
from azure.identity import DefaultAzureCredential #pip install azure-identity
from azure.identity import AzureCliCredential

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
    server = "azsqlserverejcampaignmanager.database.windows.net"
    database = "devazsqldbejcampaignmanager"

    # Get Azure AD token
    #credential = DefaultAzureCredential()
    token = credential.get_token("https://database.windows.net/.default")
    token_bytes = bytes(token.token, "utf-8")

    conn_str = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={server};Database={database};"
        f"Authentication=ActiveDirectoryAccessToken;"
        f"Encrypt=yes;TrustServerCertificate=no;"
    )

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
