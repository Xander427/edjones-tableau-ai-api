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
from fastapi.middleware.cors import CORSMiddleware 
import openai

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://tableau2.digital.accenture.com/"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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

# --- Pydantic model for requests ---
class AskRequest(BaseModel):
    question: str

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


# --- Azure OpenAI Setup ---
openai.api_type = "azure"
openai.api_base = os.getenv("AZURE_OPENAI_ENDPOINT")  # e.g., https://my-openai-resource.openai.azure.com/
openai.api_key = os.getenv("AZURE_OPENAI_KEY")
openai.api_version = "2024-05-01-preview"  # or your deployed model API version


class AIQueryRequest(BaseModel):
    query: str


@app.post("/ai_query")
async def ai_query(payload: AIQueryRequest):
    user_query = payload.query

    if not user_query:
        return {"error": "No query provided."}

    # --- Step 1: Ask Azure OpenAI to generate SQL ---
    prompt = f"""
    You are a data assistant. Convert this natural-language question into a safe SQL query 
    for Microsoft SQL Server. The database has views like v_CampaignManager, v_FacebookPaidSocial, etc.
    But you probably only need view v_TableauData_30Days and table Tableau_31DaysandOlder.
    Query: {user_query}
    """

    response = openai.ChatCompletion.create(
        engine="gpt-4o-mini",  # or your deployed model
        messages=[{"role": "system", "content": "You are a helpful SQL assistant."},
                  {"role": "user", "content": prompt}],
        temperature=0,
        max_tokens=200
    )

    sql_query = response.choices[0].message["content"].strip()

    # --- Step 2: Run SQL (safely) ---
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        return {"query": user_query, "sql": sql_query, "error": str(e)}

    # --- Step 3: Summarize results ---
    summary_prompt = f"Summarize these results briefly:\n{results}"
    summary = openai.ChatCompletion.create(
        engine="gpt-4o-mini",
        messages=[{"role": "user", "content": summary_prompt}],
        temperature=0.2
    ).choices[0].message["content"]

    return {
        "query": user_query,
        "sql": sql_query,
        "summary": summary,
        "rows": results[:10]  # show only top rows
    }