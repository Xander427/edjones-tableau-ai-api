from pydantic import BaseModel
from fastapi import FastAPI
import pyodbc
from azure.identity import DefaultAzureCredential #pip install azure-identity
from azure.identity import AzureCliCredential
import struct
import platform
pyodbc.pooling = True # Testing True
import time
import logging
import os
from fastapi.middleware.cors import CORSMiddleware 
from openai import AzureOpenAI
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

# --- Fix Azure App Service proxy bug ---
for var in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"]:
    os.environ.pop(var, None)
#That will prevent Azure’s system proxy from being passed into the client’s constructor.

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
# ---- Custom httpx client with no proxies ----
transport = httpx.HTTPTransport(retries=3)
http_client = httpx.Client(transport=transport, timeout=60)

credential = DefaultAzureCredential()

def token_provider():
    token = credential.get_token("https://cognitiveservices.azure.com/.default")
    return token.token  # return just the string

client = AzureOpenAI(
    azure_ad_token_provider=token_provider,  # <-- instead of api_key
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version="2025-01-01-preview",
    http_client=http_client
)

#client = AzureOpenAI(
#    api_key=os.getenv("AZURE_OPENAI_KEY"),
#    api_version="2025-01-01-preview",
#    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
#    http_client=http_client
#)

class AIQueryRequest(BaseModel):
    query: str


@app.post("/ai_query")
async def ai_query(payload: AIQueryRequest):
    user_query = payload.query

    if not user_query:
        return {"error": "No query provided."}
    
    schema_info = """
    The database contains advertising campaign performance data with the following tables (table name: description):
        v_TableauData_30Days: view with last 31 days of data. This table is not necessary if querying data older than 31 days.
        Tableau_31DaysandOlder: table with data older than 32 days and up to 25 months old.
    
    These two tables have identical columns. Relevant columns and their descriptions are outlined below (column name: description):
        date: day,
        Campaign: campaign,
        channel: media channel. Values include Connected TV, Paid Search, Article, TV, Skimms IG, Video - Pre-Roll, Display, None, Podcast, Paid Social, YouTube, Native, Video, Newsletter, Audio, DOOH
        AdSiteName: site of advertisement,
        FunnelStrategy: funnel strategy,
        journeyPhase: journey/funnel location. Values include Pre-Explore Awareness, None, Evaluate, Explore, Pre-Explore Familiarity,
        Platform: Online Video platform (Hulu, Netflix, etc.), Publication (Meredith, WSJ, etc.), Audio Streaming site (Pandora, Spotify, etc.), Social Media platform (Instagram, Pinterest, etc.), etc.,
        Placementobjective: objective of the ad buy,
        Budget Source: funding/budget source,
        IA Target: target income level. Values include None, HHI 30%, 75k, 100-249k, 250k, 50k,
        Geographic: geography. Values include Designated Market Areas, National, High Net Worth, None, Local,
        Target Audience: target audience,
        Campaign Objective: ad objective. Values include Engagement, FA Lookup, Everfi Learners, Conversions, Prospect, Site Traffic, None, Awareness, Leads,
        callcount: number of calls,
        clicks: number of clicks,
        impressions: number of impressions,
        mediaCost: media spend/budget,
        siteVisits: number of site visits,
        videoFullyPlayed: videos played completely, 100%,
        videoViews: video views,
        Engaged Visits: engaged visits,
        Leads: leads (applies only to pinterest data)
    """

    # --- Step 1: Ask Azure OpenAI to generate SQL ---
    prompt = f"""
    You are a data assistant. Convert this natural-language question into a safe SQL query 
    for Microsoft SQL Server. All data is stored in two tables with the following schema:
    {schema_info}

    Return only **valid SQL**, do not include explanations, comments, or markdown.
    Do not include any text outside the SQL query.
    Queries that reference both tables should use a UNION ALL in a subquery, e.g., 
    SELECT ... 
    FROM (
        SELECT ... 
        FROM v_TableauData_30Days  
        UNION ALL
        SELECT ...
        FROM Tableau_31DaysandOlder
    ) AS CombinedData
    Acronyms: CPL = Cost Per Lead, CTR = Click-Through Rate (clicks / impressions), CPEV = Cost Per Engaged Visit

    User question: {user_query}
    """

    response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You are a helpful SQL assistant."},
        {"role": "user", "content": prompt}
    ],
    temperature=0,
    max_tokens=200
)

    sql_query = response.choices[0].message.content.strip()

    # --- Step 2: Run SQL (safely) ---
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        return {
            "query": user_query,
            "sql": sql_query,
            "error": str(e)[:300],  # truncate long ODBC errors
            "summary": "The query could not be executed. Please rephrase or simplify."
        }

    # --- Step 3: Summarize results ---
    summary_prompt = f"Summarize these results briefly:\n{results}"
    summary = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": summary_prompt}],
        temperature=0.2
    ).choices[0].message.content

    # --- Step 4: log to table ---
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO Tableau_AI_QueryLog (user_query, sql_generated, rows_returned, summary, tableau_user)
            VALUES (?, ?, ?, ?, ?)
        """, (user_query, sql_query, len(results), summary[:4000], "Unknown"))  # or detected user
        conn.commit()
    except Exception as log_err:
        print("Logging failed:", log_err)

    return {
        "query": user_query,
        "sql": sql_query,
        "summary": summary,
        "rows": results[:10]  # show only top rows
    }