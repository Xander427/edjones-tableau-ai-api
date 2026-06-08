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

    # Normalize Unicode en/em dashes to ASCII hyphen for date range matching
    query_for_dates = re.sub(r'[–—]', '-', query_lower)

    # ==== NUMERIC DATE RANGE (most specific) e.g. "1/6/26 - 2/14/26" ====
    numeric_range = re.search(
        r'\b(\d{1,2})/(\d{1,2})/(\d{2,4})\s*-+\s*(\d{1,2})/(\d{1,2})/(\d{2,4})\b',
        query_for_dates
    )
    if numeric_range:
        def _y(s): y = int(s); return 2000 + y if y < 100 else y
        return {"field": "date", "values": [
            str(date(_y(numeric_range.group(3)), int(numeric_range.group(1)), int(numeric_range.group(2)))),
            str(date(_y(numeric_range.group(6)), int(numeric_range.group(4)), int(numeric_range.group(5))))
        ]}

    # ==== WEEK RANGE same month e.g. "Mar 16-22, 2026" ====
    week_range = re.search(
        r'\b(january|february|march|april|may|june|july|august|september|october|november|december)'
        r'\s+(\d{1,2})\s*-+\s*(\d{1,2}),?\s+(20\d{2})\b',
        query_for_dates
    )
    if week_range:
        m = month_names[week_range.group(1)]
        y = int(week_range.group(4))
        return {"field": "date", "values": [
            f"{y}-{m:02d}-{int(week_range.group(2)):02d}",
            f"{y}-{m:02d}-{int(week_range.group(3)):02d}"
        ]}

    # ==== CROSS-MONTH DATE RANGE e.g. "Apr 8 - Jun 2, 2025" or "Jan 6, 2026 - Feb 14, 2026" ====
    cross_month_range = re.search(
        r'\b(january|february|march|april|may|june|july|august|september|october|november|december)'
        r'\s+(\d{1,2})(?:,?\s+(20\d{2}))?\s*-+\s*'
        r'(january|february|march|april|may|june|july|august|september|october|november|december)'
        r'\s+(\d{1,2}),?\s+(20\d{2})\b',
        query_for_dates
    )
    if cross_month_range:
        end_year = int(cross_month_range.group(6))
        start_year = int(cross_month_range.group(3)) if cross_month_range.group(3) else end_year
        s_m = month_names[cross_month_range.group(1)]
        e_m = month_names[cross_month_range.group(4)]
        return {"field": "date", "values": [
            f"{start_year}-{s_m:02d}-{int(cross_month_range.group(2)):02d}",
            f"{end_year}-{e_m:02d}-{int(cross_month_range.group(5)):02d}"
        ]}

    # ==== MONTH-TO-MONTH RANGE e.g. "March 2026 and April 2026", "January and April 2026", "in 2026 between January and April" ====
    month_range = re.search(
        r'\b(january|february|march|april|may|june|july|august|september|october|november|december)'
        r'(?:\s+(20\d{2}))?\s+(?:and|through|to|-)\s+'
        r'(january|february|march|april|may|june|july|august|september|october|november|december)'
        r'(?:\s+(20\d{2}))?\b',
        query_for_dates
    )
    if month_range:
        s_m = month_names[month_range.group(1)]
        e_m = month_names[month_range.group(3)]
        year_str = month_range.group(4) or month_range.group(2)
        if not year_str:
            y_match = re.search(r'\b(20\d{2})\b', query_for_dates)
            year_str = y_match.group(1) if y_match else None
        if year_str:
            end_year = int(year_str)
            start_year = int(month_range.group(2)) if month_range.group(2) else end_year
            last_day = calendar.monthrange(end_year, e_m)[1]
            return {"field": "date", "values": [
                f"{start_year}-{s_m:02d}-01",
                f"{end_year}-{e_m:02d}-{last_day:02d}"
            ]}

    # ==== MONTH DETECTION (month + year) ====
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

    # Expand Journey Phase abbreviations so PREA/PREF match the full FILTER_MAP values
    query_normalized = re.sub(r'\bprea\b', 'pre-explore awareness', query_lower)
    query_normalized = re.sub(r'\bpref\b', 'pre-explore familiarity', query_normalized)

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

            if re.search(pattern, query_normalized):
                if any(val_lower in m for m in matched_text):
                    continue

                matched_values.append(value)
                matched_text.add(val_lower)

        if matched_values:
            # Prevent "Video" channel from matching when the user is asking about a video metric
            if field == "Channel" and "Video" in matched_values:
                video_metric_pattern = r'\b(video views|video plays|video completion|vcr|video completes?|video completion rate)\b'
                if re.search(video_metric_pattern, query_lower):
                    matched_values.remove("Video")
            if matched_values:
                filters[field] = matched_values

    # NEW: Extract date range
    date_filter = parse_date_from_query(user_query)
    if date_filter:
        filters[date_filter["field"]] = date_filter["values"]
        print(f"Detected date filter: {date_filter['values']}")

    # Measure Names filter — only for metrics not shown by default in the dashboard.
    measure_names = []
    # Leads: excluded from default view; add when asked (but not for CPL/cost per lead queries)
    if re.search(r'\bleads?\b', query_lower) and not re.search(r'\b(cp lead|cost per lead|cpl)\b', query_lower):
        measure_names.append("Leads")
    # VCR: not in default view; add when asked (but not for CPCV/cost per completed view queries)
    if re.search(r'\bvcr\b|\bvideo completion rate\b', query_lower) and not re.search(r'\bcpcv\b|\bcost per completed view\b', query_lower):
        measure_names.append("VCR")
    if measure_names:
        filters["Measure Names"] = measure_names

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
        journeyPhase: journey/funnel location. Values include Pre-Explore Awareness (aka PREA), None, Evaluate, Explore, Pre-Explore Familiarity (aka PREF).
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
        siteVisits: direct site visits recorded at the placement level.
        TotalConversions: conversion count used for CM Floodlight and LinkedIn-sourced site visits.
        ConversionTagName: name of the conversion tag. Rows where this field matches specific CM Floodlight tag names represent additional site visits.
        Site Visits (combined, matches Tableau dashboard): ALWAYS use this formula — never use siteVisits alone — when the user asks about site visits or CPSV:
            ISNULL(SUM(siteVisits), 0)
            + ISNULL(SUM(CASE WHEN ConversionTagName IN (
                'Floodlight - ACC - EJ Investor All Pages',
                'ejgen : Floodlight - ACC - EJ Investor All Pages: Paid Search Actions',
                'Floodlight - ACC - Match Tool - Landing Page',
                'Floodlight - ACC - Starting Point - Homepage',
                'MatchTool : Floodlight - ACC - Match Tool - Landing Page: Paid Search Actions',
                'Startp002 : Floodlight - ACC - Starting Point - Homepage: Paid Search Actions',
                'FLOODLIGHT_ACC_EJ_INVESTOR_ALL_PAGES',
                'FLOODLIGHT_ACC_MATCH_TOOL_LANDING_PAGE',
                'FLOODLIGHT_ACC_STARTING_POINT_HOMEPAGE',
                'EJGEN_FLOODLIGHT_ACC_EJ_INVESTOR_ALL_PAGES_PAID_SEARCH_ACTIONS',
                'MATCHTOOL_FLOODLIGHT_ACC_MATCH_TOOL_LANDING_PAGE_PAID_SEARCH_ACTIONS',
                'STARTP002_FLOODLIGHT_ACC_STARTING_POINT_HOMEPAGE_PAID_SEARCH_ACTIONS'
            ) THEN TotalConversions ELSE 0 END), 0)
            + ISNULL(SUM(CASE WHEN tablename = 'v_LinkedInCampaign' THEN TotalConversions ELSE 0 END), 0)
        videoFullyPlayed: videos played completely, 100%. Frequently referred to as "Video Completes".
        videoViews: video view count for non-YouTube platforms.
        VideoPlays: video play/start count, primarily for YouTube and other platforms.
        tablename: source table identifier. The value 'v_YouTubePaidMedia' identifies YouTube rows.
        mediaBuyName: name of the media buy/placement.
        Video Views (calculated): The correct formula for "Video Views" matches the Tableau dashboard and MUST be used whenever the user asks for video views, video plays, or any metric whose denominator is video views (VCR, CPCV). Formula: COALESCE(SUM(videoViews),0) + COALESCE(SUM(VideoPlays),0) + COALESCE(SUM(CASE WHEN tablename='v_YouTubePaidMedia' AND (mediaBuyName LIKE '%NonSkippable%' OR mediaBuyName LIKE '%Bumper%') THEN impressions ELSE 0 END),0)
        Viewability: calculated as SUM([Viewable Impressions]) / SUM([Measured Impressions]). Use this formula directly when asked for viewability — do not use these two columns for anything else.
        [Engaged Visits]: engaged visits. IMPORTANT: this column name contains a space and MUST always be referenced as [Engaged Visits] in SQL.
        CallCount: number of calls (Google only).
        Leads: leads (applies only to pinterest data).
    """

    # --- Step 1: Ask Azure OpenAI to generate SQL ---
    prompt = f"""
    You are a data assistant. Convert this natural-language question into a safe SQL query 
    for Microsoft SQL Server. All data is stored in two tables with the following schema:
    {schema_info}

    Return only **valid SQL**, do not include explanations, comments, or markdown.
    Do not include any text outside the SQL query.
    Queries that reference both tables should use a UNION ALL in a subquery. You MUST include AND FunnelStrategy NOT IN ('Null', 'NA', 'Quarter 2') in the WHERE clause of EVERY inner subquery — no exceptions, even if FunnelStrategy is not selected or grouped:
    SELECT ...
    FROM (
        SELECT ...
        FROM v_TableauData_30Days
        WHERE <date or other conditions> AND FunnelStrategy NOT IN ('Null', 'NA', 'Quarter 2')
        UNION ALL
        SELECT ...
        FROM Tableau_31DaysandOlder
        WHERE <date or other conditions> AND FunnelStrategy NOT IN ('Null', 'NA', 'Quarter 2')
    ) AS CombinedData
    Acronyms: CPC = Cost Per Click, CPL = Cost Per Lead, CTR = Click-Through Rate (clicks / impressions), CPEV = Cost Per Engaged Visit, CPM = Cost Per Mille (Cost per 1000 Impressions), CPSV = Cost Per Site Visit, CPCV = Cost Per Completed View, VCR = Video Completion Rate (also called Audio Completion Rate for audio placements — same formula), EV = Engaged Visits, TTD = The Trade Desk. CPV = Cost Per View = CPCV.
    For CP EV / CPEV calculations, use: SUM(mediaCost) / NULLIF(SUM([Engaged Visits]), 0). Note: [Engaged Visits] must always be in square brackets.
    For CPSV (Cost Per Site Visit) and any site visit count, NEVER use SUM(siteVisits) alone — always use the combined Site Visits formula from the schema above: ISNULL(SUM(siteVisits),0) + ISNULL(SUM(CASE WHEN ConversionTagName IN (...12 floodlight values...) THEN TotalConversions ELSE 0 END),0) + ISNULL(SUM(CASE WHEN tablename='v_LinkedInCampaign' THEN TotalConversions ELSE 0 END),0). CPSV = SUM(mediaCost) / NULLIF(<combined site visits>, 0).
    For CPL / CP Lead (Cost Per Lead) calculations, use: SUM(mediaCost) / NULLIF(SUM(Leads), 0).
    For CPC (Cost Per Click) calculations, use: SUM(mediaCost) / NULLIF(SUM(clicks), 0). IMPORTANT: CPC (Cost Per Click) is completely different from CPCV (Cost Per Completed View) — never use videoFullyPlayed for CPC.
    For CPCV (Cost Per Completed View) calculations, use: SUM(mediaCost) / NULLIF(SUM(videoFullyPlayed), 0).
    For VCR (Video Completion Rate) calculations, use: SUM(videoFullyPlayed) / NULLIF(<Video Views formula>, 0) where <Video Views formula> = COALESCE(SUM(videoViews),0) + COALESCE(SUM(VideoPlays),0) + COALESCE(SUM(CASE WHEN tablename='v_YouTubePaidMedia' AND (mediaBuyName LIKE '%NonSkippable%' OR mediaBuyName LIKE '%Bumper%') THEN impressions ELSE 0 END),0).
    For CPM calculations, use the weighted average formula: SUM(mediaCost) * 1000.0 / NULLIF(SUM(impressions), 0). Do NOT add a WHERE impressions > 0 filter — non-impression channels (Audio, Podcast, Paid Search) have impressions = 0 and their spend must still be included in the mediaCost numerator. NULLIF handles division by zero.
    Note that there is a 1 day lag in data availability. We don't have any data for today. I.e., if today is June 10, the most recent data in the database is for June 9.
    INTERVAL should not be used for date ranges (it is not valid SQL); use DATEADD and DATEDIFF functions instead.
    Integers cannot be added to dates in SQL code like: WHERE date < '2025-01-01' + 365; use DATEADD and DATEDIFF functions instead.
    When a user asks about a specific brand, network, or service by name (e.g., ESPN, CNBC, Pandora, Spotify, Disney, Hulu, Netflix, Instagram, Nativo, SiriusXM, YouTube, Facebook, Roku, LinkedIn, Pinterest, Bloomberg, NBC, ABC, CBS, FOX, Amazon), always filter by the Platform or Publisher column — NEVER by channel. The channel column only contains broad media types: Connected TV, Paid Search, Paid Social, Display, Video, Audio, TV, Podcast, Native, Article, Newsletter, DOOH.
    Column names that contain spaces must be wrapped in square brackets, e.g., [Engaged Visits], [Keyword Type], [Budget Source].
    Unless the user specifically asks about FunnelStrategy 'Null', 'NA', or 'Quarter 2', always exclude those rows by default using WHERE FunnelStrategy NOT IN ('Null', 'NA', 'Quarter 2').
    When grouping or filtering by Journey Phase (journeyPhase), always exclude rows where journeyPhase = 'None'.
    When ordering results by Journey Phase, always use this order via a CASE expression in ORDER BY: Pre-Explore Awareness = 1, Pre-Explore Familiarity = 2, Explore = 3, Evaluate = 4.
    When the user asks to see results 'by [dimension]' (e.g., 'by Journey Phase', 'by Platform', 'by month', 'by Channel'), always include that dimension in both SELECT and GROUP BY. For 'by month', use FORMAT(date, 'yyyy-MM') AS Month in SELECT and GROUP BY, ordered by Month. When asked for 'top N per month' (e.g., 'top 3 platforms by month'), use a CTE to compute monthly totals per dimension, then apply ROW_NUMBER() OVER (PARTITION BY Month ORDER BY metric DESC) and filter WHERE rn <= N in the outer SELECT.
    When the user specifies a channel type (e.g., 'Paid Social platform', 'Display publisher', 'Connected TV placement'), filter by channel = 'channel_value' (e.g., WHERE channel = 'Paid Social') — never match channel type names against the Platform or Publisher columns. The channel column holds broad media types; Platform and Publisher hold specific vendor/network names within a channel.
    When filtering Publisher or Platform by a name that may have variants (e.g., 'Hulu' could match 'Hulu', 'Hulu Slate', 'Hulu DSE'), use LIKE '%name%' in the WHERE clause to include all variants.
    Direction rules — always apply ORDER BY and TOP N to return only what the user asked for:
      - For volume metrics (impressions, clicks, leads, site visits, engaged visits): "highest/most/top" = ORDER BY metric DESC; "lowest/fewest/least" = ORDER BY metric ASC.
      - For cost-efficiency metrics (CPM, CPC, CPL, CPSV, CPEV, CPCV): LOWER values are better. "lowest/cheapest/most efficient/best" = ORDER BY metric ASC; "highest/most expensive/worst" = ORDER BY metric DESC.
      - Use SELECT TOP 1 when asking for a single winner; TOP N when the user specifies a count (e.g., "top 3").
    When querying any metric over a time period (month, quarter, year), always use a date range with >= and < (e.g., date >= '2025-10-01' AND date < '2025-11-01'). Never use a single date equality filter (WHERE date = 'YYYY-MM-DD') unless the user explicitly asks about a specific single day.
    For period-over-period comparisons (month-over-month, year-over-year, or any "change from X to Y"): compute both periods in a single query using conditional aggregation with CASE WHEN inside a CTE, then calculate the difference in the outer SELECT. For simple metrics: SUM(CASE WHEN date >= 'A_start' AND date < 'A_end' THEN column ELSE 0 END) AS period_A. For weighted-average metrics like CPM, compute numerator and denominator separately per period: SUM(CASE WHEN period_A THEN mediaCost ELSE 0 END)*1000.0/NULLIF(SUM(CASE WHEN period_A THEN impressions ELSE 0 END),0) AS CPM_A. Percent change: (metric_B - metric_A) / NULLIF(metric_A, 0) * 100. Always label columns clearly (e.g., CPM_March, CPM_April, CPM_Change, CPM_PctChange).

    User question: {user_query}
    """

    response = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[
        {"role": "system", "content": "You are a helpful SQL assistant."},
        {"role": "user", "content": prompt}
    ],
    temperature=0,
    max_tokens=1000
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
    summary_prompt = f"""Summarize these results briefly for a user who asked: "{user_query}"
IMPORTANT: Use only these acronym definitions — do not invent alternatives:
VCR = Video Completion Rate (NEVER "Value Creation Ratio" or any other meaning)
CPCV = Cost Per Completed View, CPM = Cost Per Mille, CPL = Cost Per Lead,
CPEV = Cost Per Engaged Visit, CPSV = Cost Per Site Visit, CTR = Click-Through Rate.
Formatting rules:
- CTR, VCR, and Viewability are ratio metrics stored as decimals — always display them as percentages rounded to 2 decimal places (e.g., 0.09608 → 9.61%, 0.8043 → 80.43%).
- All cost metrics (CPM, CPC, CPL, CPSV, CPEV, CPCV, mediaCost) should be prefixed with $.
- For period-over-period results, clearly label each period and show the change and percent change.
- When results include Publisher or Platform variants as separate rows (e.g., 'Hulu' and 'Hulu Slate' and 'Hulu DSE'), list each variant separately — never merge or consolidate them into a single entry.
Results:
{results}"""
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