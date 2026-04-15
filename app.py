from pydantic import BaseModel
from fastapi import FastAPI, Request, HTTPException
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
import re
from datetime import datetime, date, timedelta
import calendar
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

# --- Fix Azure App Service proxy bug ---
for var in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY"]:
    os.environ.pop(var, None)
#That will prevent Azure’s system proxy from being passed into the client’s constructor.

app = FastAPI()

origins = [
    "https://witty-bush-00501930f.3.azurestaticapps.net",  # static web app hostname
    "https://tableau2.digital.accenture.com"  # Tableau Server hostname
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

#ensure Tableau-only access
@app.middleware("http")
async def require_tableau(request: Request, call_next):

    # Allow CORS preflight and Azure health checks
    if request.method == "OPTIONS" or request.url.path == "/":
        return await call_next(request)

    # Enforce Tableau-only header
    tableau_flag = request.headers.get("X-Tableau-Extension")

    if tableau_flag != "true":
        raise HTTPException(
            status_code=403,
            detail="Unauthorized use"
        )

    return await call_next(request)

# --- Healthcheck endpoint ---
@app.get("/")
def healthcheck():
    return {"status": "ok"}

# --- Helper functions ---
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


def parse_date_from_query(query: str):
    """Extract date range from natural language query"""
    query_lower = query.lower()
    today = date.today()
    
    # ==== MONTH DETECTION (most specific) ====
    month_names = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }
    
    month_match = re.search(r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+(20\d{2})\b', query_lower)
    if month_match:
        month_name = month_match.group(1)
        month_num = month_names[month_name]
        year = int(month_match.group(2))
        
        start_date = f"{year}-{month_num:02d}-01"
        last_day = calendar.monthrange(year, month_num)[1]
        end_date = f"{year}-{month_num:02d}-{last_day:02d}"
        
        return {
            "field": "date",
            "values": [start_date, end_date]
        }
    
    # ==== QUARTER DETECTION ====
    quarter_match = re.search(r'\b(q[1-4]|quarter\s+[1-4])\s+(20\d{2})\b', query_lower)
    if quarter_match:
        quarter = int(re.search(r'[1-4]', quarter_match.group(1)).group())
        year = int(quarter_match.group(2))
        
        month_map = {1: 1, 2: 4, 3: 7, 4: 10}
        start_month = month_map[quarter]
        end_month = start_month + 2
        
        start_date = f"{year}-{start_month:02d}-01"
        last_day = calendar.monthrange(year, end_month)[1]
        end_date = f"{year}-{end_month:02d}-{last_day:02d}"
        
        return {
            "field": "date",
            "values": [start_date, end_date]
        }
    
    # ==== RELATIVE DATES ====
    if "last month" in query_lower:
        if today.month == 1:
            start_date = date(today.year - 1, 12, 1)
        else:
            start_date = date(today.year, today.month - 1, 1)
        
        end_date = date(today.year, today.month, 1) - timedelta(days=1)
        
        return {
            "field": "date",
            "values": [str(start_date), str(end_date)]
        }
    
    if "last quarter" in query_lower:
        current_quarter = (today.month - 1) // 3 + 1
        if current_quarter == 1:
            quarter = 4
            year = today.year - 1
        else:
            quarter = current_quarter - 1
            year = today.year
        
        month_map = {1: 1, 2: 4, 3: 7, 4: 10}
        start_month = month_map[quarter]
        end_month = start_month + 2
        
        start_date = f"{year}-{start_month:02d}-01"
        last_day = calendar.monthrange(year, end_month)[1]
        end_date = f"{year}-{end_month:02d}-{last_day:02d}"
        
        return {
            "field": "date",
            "values": [start_date, end_date]
        }
    
    # ==== YEAR-TO-DATE ====
    if "ytd" in query_lower or "year to date" in query_lower:
        start_date = f"{today.year}-01-01"
        end_date = str(today)
        return {
            "field": "date",
            "values": [start_date, end_date]
        }
    
    # ==== BARE MONTH (no year specified - default to most recent occurrence) ====
    bare_month_match = re.search(r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\b', query_lower)
    if bare_month_match and not re.search(r'\b(20\d{2})\b', query_lower):
        month_name = bare_month_match.group(1)
        month_num = month_names[month_name]
        # If the month has already passed or is current this year, use this year; otherwise last year
        year = today.year if month_num <= today.month else today.year - 1
        start_date = f"{year}-{month_num:02d}-01"
        last_day = calendar.monthrange(year, month_num)[1]
        end_date = f"{year}-{month_num:02d}-{last_day:02d}"
        return {"field": "date", "values": [start_date, end_date]}

    # ==== YEAR DETECTION (least specific - run last) ====
    year_match = re.search(r'\b(20\d{2})\b', query_lower)
    if year_match:
        year = int(year_match.group(1))
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
        return {
            "field": "date",
            "values": [start_date, end_date]
        }
    
    return None

#enhanced filter extraction to include value prioritization and date ranges
def extract_filters_from_query(user_query: str):
    """Enhanced to include date range detection"""
    filters = {}
    query_lower = user_query.lower()

    # Extract standard categorical filters (your existing logic)
    for field, values in FILTER_MAP.items():
        if values == "RANGE":  # Skip range filters in value matching
            continue
            
        # ... your existing categorical filter matching logic ...
        # (keep everything you currently have for Publisher, Platform, etc.)
        sorted_values = sorted(values, key=lambda v: len(v or ""), reverse=True)
        
        matched_values = []
        matched_text = set()

        for value in sorted_values:
            if not value or value.lower() == "none":
                continue

            val_lower = value.lower()
            pattern = r"\b" + re.escape(val_lower) + r"\b"

            if re.search(pattern, query_lower):
                if any(val_lower in m for m in matched_text):
                    continue

                matched_values.append(value)
                matched_text.add(val_lower)

        if matched_values:
            filters[field] = matched_values

    # NEW: Extract date range
    date_filter = parse_date_from_query(user_query)
    if date_filter:
        filters[date_filter["field"]] = date_filter["values"]
        print(f"Detected date filter: {date_filter['values']}")

    return filters


def sanitize_user_query(text: str) -> str:
    if not text:
        return ""
    
    # Remove control characters
    text = re.sub(r"[\x00-\x1F\x7F]", " ", text)

    # Collapse repeated whitespace
    text = re.sub(r"\s+", " ", text)

    # Limit length (prevents abuse)
    return text[:2000].strip()


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
    
# --- Tableau Filter Map ---

FILTER_MAP = {
  # "Measure Names" intentionally excluded — it is a Tableau system field that controls
  # which measures are displayed in a view, not a data dimension. Including it caused
  # over-filtering on metric queries (leads, impressions, etc.).

  "Branded": ["Brand", "Non-Brand"],

  "Geography": ["Dallas", "DC", "National", "None", "Seattle"],

  "Publisher   ": [
    "Null", "ABC", "Amazon", "Bing", "Bloomberg", "Business Insider", "CBS", "CNBC",
    "Conde Nast", "Demand Gen", "Discovery", "Disney", "Disney DSE", "DV360", "ENT",
    "ESP2", "ESPN", "ESPN DSE", "Facebook", "FBN", "Forbes", "FOX", "FS1", "GOLF",
    "Google", "HTS", "Hulu", "Hulu DSE", "Hulu Slate", "Instagram", "Investing.com",
    "LinkedIn", "Meredith", "Nasdaq", "Nativo", "NBAT", "NBC", "Netflix", "NGC",
    "None", "NPR", "Pandora", "Paramount", "PARB", "PARC", "Pinterest", "Reuters",
    "Roku", "She Media", "Sirius XM", "Spotify", "SWYM", "TBS", "The Street",
    "The Trade Desk", "TNT", "Triplelift", "TRU", "Uber", "USA", "USA Today", 
    "Vox", "Wall Street Journal", "YouTube"
  ],

  "Brand vs NB": ["Brand", "Non-Brand", "Null"],

  "Main Audience Group": [
    "$250K - Adults 25-64, $250K+ IA",
    "$250K - Adults 30-49, $250K+ IA",
    "BrandA25-64, HHI $75K+",
    "Other",
    "Women - W30-49, HHI $75k+"
  ],

  "SubAudience1": [
    "Mindset - Career Changer", "Mindset - Generic", "Mindset - Golden Years",
    "Mindset - Life Improvers", "Mindset - Money Maker", "Other"
  ],

  "SubAudience2": [
    "FAN LAL", "Other", "Platform Lookalike", "Website Retargeting"
  ],

  "Campaign Category": ["250K", "EdWoW", "GenNext", "Investor", "NA", "PIC", "PII"],

  "FunnelStrategy": ["Null", "Brand", "NA", "Performance", "Quarter 2"],

  "Platform": [
    "Null", "ABC", "Amazon", "Bing", "Bleacher Report", "CBS", "CNBC",
    "Discovery Plus", "Disney", "DV360", "ENT", "ESP2", "ESPN", "Facebook",
    "FBN", "FOX", "FS1", "GOLF", "Google", "HTS", "Hulu", "Instagram",
    "LinkedIn", "Meredith", "NASDAQ", "Nativo", "NBAT", "NBC", "Netflix",
    "NGC", "NPR", "Pandora", "Paramount", "PARB", "PARC", "Pinterest",
    "She Media", "SiriusXM", "SoundCloud", "Spotify", "TBS",
    "The Street Editorial", "The Trade Desk", "TheSkimm", "TNT", "TRU",
    "USA", "Vox", "WSJ"
  ],

  "Targeting Strategy": [
    "Null", "1st Party Audience Data", "Behavioral Targeting",
    "Contextual Targeting", "Demographic Targeting Only",
    "Google Affinity Data", "Google Custom Affinity",
    "Google Custom Intent", "Google In Market", "Hyper Local Targeting",
    "Keyword Contextual", "Lookalike Modeling",
    "Multiple Targeting Methods", "None", "Recency RTG",
    "Retargeting Targeting", "Run of Network Targeting",
    "Run of Site Targeting", "Specific Site List", "Topic Targeting",
    "Video Retargeting", "Website Retargeting"
  ],

  "Channel": [
    "Article", "Audio", "Connected TV", "Display", "DOOH", "Native",
    "Newsletter", "None", "Paid Search", "Paid Social", "Podcast",
    "Skimms IG", "TV", "Video", "Video - Pre-Roll", "YouTube"
  ],

  "Journey Phase": [
    "Evaluate", "Explore", "None", "Pre-Explore Awareness", "Pre-Explore Familiarity"
  ],

    #DATE is a range filter, so no domain list needed
  "date": "RANGE",

  "Date Granularity": ["Year", "Quarter", "Month", "Week", "Day"]
}


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

class AIQueryRequest(BaseModel):
    query: str
    client_id: Optional[str] = None


@app.post("/ai_query")
async def ai_query(payload: AIQueryRequest):

    user_query = sanitize_user_query(payload.query)

    client_id = payload.client_id or "unknown"  #uuid retreived from index.html based on client browser
    
    schema_info = """
    The database contains advertising campaign performance data with the following tables (table name: description):
        v_TableauData_30Days: view with data 1-31 days old. 
        Tableau_31DaysandOlder: table with data older than 32 days and up to 25 months old.
    
    These two tables have identical columns. Relevant columns and their descriptions are outlined below (column name: description):
        date: day.
        Campaign: campaign category. Valid values are: 250K, EdWoW, GenNext, Investor, NA, PIC, PII. IMPORTANT: '250K' is a campaign name, not a dollar amount — never interpret it as a numeric threshold.
        channel: media channel. Values include Connected TV, Paid Search, Article, TV, Skimms IG, Video - Pre-Roll, Display, None, Podcast, Paid Social, YouTube, Native, Video, Newsletter, Audio, DOOH.
        FunnelStrategy: funnel strategy. Valid values are: Brand and Performance. NULL, NA, and Quarter 2 should not be queried unless specified.
        journeyPhase: journey/funnel location. Values include Pre-Explore Awareness, None, Evaluate, Explore, Pre-Explore Familiarity.
        Platform: Values include ABC, Amazon, Bing, Bleacher Report, CBS, CNBC, Discovery Plus, Disney, DV360, ENT, ESP2, ESPN, Facebook, FBN, FOX, FS1, GOLF, Google, HTS, Hulu, Instagram,
            LinkedIn, Meredith, NASDAQ, Nativo, NBAT, NBC, Netflix, NGC, NPR, Pandora, Paramount, PARB, PARC, Pinterest, She Media, SiriusXM, SoundCloud, Spotify, TBS,
            The Street Editorial, The Trade Desk, TheSkimm, TNT, TRU, USA, Vox, WSJ
        Geography: Values include Designated Market Areas, National, High Net Worth, None, Local.
        [Targeting Strategy]: Values include Hyper Local Targeting, 1st Party Audience Data, Demographic Targeting Only,Google Custom Intent, Recency RTG, Retargeting Targeting, Lookalike Modeling, 
            Topic Targeting, Google Custom Affinity, Specific Site List, Google In Market, Keyword Contextual, Run of Site Targeting, Video Retargeting, Multiple Targeting Methods, Google Affinity Data
            None, Run of Network Targeting, Behavioral Targeting, Website Retargeting, Contextual Targeting.
        [Target Audience]: target audience.
        Publisher: BusinessInsider, Conde Nast, Discovery, Meredith, Nasdaq, Nativo, NBC, Netflix, None, Paramount, Pinterest, Roku, SiriusXM, The Trade Desk, TripleLift, USA Today, Wall Street Journal, YouTube
        callcount: number of calls.
        clicks: number of clicks.
        impressions: number of impressions.
        mediaCost: media spend/budget.
        siteVisits: number of site visits. IMPORTANT: the column is named siteVisits (camelCase, no spaces, no brackets). Do NOT use [Site Visits] or Sessioncount.
        videoFullyPlayed: videos played completely, 100%.
        videoViews: video views.
        [Engaged Visits]: engaged visits. IMPORTANT: this column name contains a space and MUST always be referenced as [Engaged Visits] in SQL.
        Leads: leads (applies only to pinterest data).
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
    Acronyms: CPL = Cost Per Lead, CTR = Click-Through Rate (clicks / impressions), CPEV = Cost Per Engaged Visit, CPM = Cost Per Mille (Cost per 1000 Impressions), CPSV = Cost Per Site Visit, CPCV = Cost Per Completed View, EV = Engaged Visits, TTD = The Trade Desk.
    For CP EV / CPEV calculations, use: SUM(mediaCost) / NULLIF(SUM([Engaged Visits]), 0). Note: [Engaged Visits] must always be in square brackets.
    For CPSV (Cost Per Site Visit) calculations, use: SUM(mediaCost) / NULLIF(SUM(siteVisits), 0).
    For CPL / CP Lead (Cost Per Lead) calculations, use: SUM(mediaCost) / NULLIF(SUM(Leads), 0).
    For CPCV (Cost Per Completed View) calculations, use: SUM(mediaCost) / NULLIF(SUM(videoFullyPlayed), 0).
    For CPM calculations, use the weighted average formula: SUM(mediaCost) * 1000.0 / NULLIF(SUM(impressions), 0). Do NOT add a WHERE impressions > 0 filter — non-impression channels (Audio, Podcast, Paid Search) have impressions = 0 and their spend must still be included in the mediaCost numerator. NULLIF handles division by zero.
    Note that there is a 1 day lag in data availability. We don't have any data for today. I.e., if today is June 10, the most recent data in the database is for June 9.
    INTERVAL should not be used for date ranges (it is not valid SQL); use DATEADD and DATEDIFF functions instead.
    Integers cannot be added to dates in SQL code like: WHERE date < '2025-01-01' + 365; use DATEADD and DATEDIFF functions instead.
    When a user asks about a specific brand, network, or service by name (e.g., ESPN, CNBC, Pandora, Spotify, Disney, Hulu, Netflix, Instagram, Nativo, SiriusXM, YouTube, Facebook, Roku, LinkedIn, Pinterest, Bloomberg, NBC, ABC, CBS, FOX, Amazon), always filter by the Platform or Publisher column — NEVER by channel. The channel column only contains broad media types: Connected TV, Paid Search, Paid Social, Display, Video, Audio, TV, Podcast, Native, Article, Newsletter, DOOH.
    Column names that contain spaces must be wrapped in square brackets, e.g., [Engaged Visits], [Keyword Type], [Budget Source].
    Unless the user specifically asks about FunnelStrategy 'Null', 'NA', or 'Quarter 2', always exclude those rows by default using WHERE FunnelStrategy NOT IN ('Null', 'NA', 'Quarter 2').
    When grouping or filtering by Journey Phase (journeyPhase), always exclude rows where journeyPhase = 'None'.
    When ordering results by Journey Phase, always use this order via a CASE expression in ORDER BY: Pre-Explore Awareness = 1, Pre-Explore Familiarity = 2, Explore = 3, Evaluate = 4.
    When filtering Publisher or Platform by a name that may have variants (e.g., 'Hulu' could match 'Hulu', 'Hulu Slate', 'Hulu DSE'), use LIKE '%name%' in the WHERE clause to include all variants.
    When querying any metric over a time period (month, quarter, year), always use a date range with >= and < (e.g., date >= '2025-10-01' AND date < '2025-11-01'). Never use a single date equality filter (WHERE date = 'YYYY-MM-DD') unless the user explicitly asks about a specific single day.

    User question: {user_query}
    """

    response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You are a helpful SQL assistant."},
        {"role": "user", "content": prompt}
    ],
    temperature=0,
    max_tokens=500
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
        """, (user_query, sql_query, len(results), summary[:4000], client_id))  
        conn.commit()
    except Exception as log_err:
        print("Logging failed:", log_err)
    
    # --- Step 5: create Tableau filters ---
    filters = extract_filters_from_query(user_query)

    return {
        "query": user_query,
        "sql": sql_query,
        "summary": summary,
        "rows": results[:25],  # show only top rows
        "filters": filters
    }