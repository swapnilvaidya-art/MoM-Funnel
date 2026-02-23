import os
import time
import json
import math
import requests
import pandas as pd
import numpy as np
import gspread
from google.oauth2.service_account import Credentials

# -------------------- START TIMER --------------------
start_time = time.time()

# -------------------- ENV VARIABLES --------------------
sec = os.getenv("SWAPNIL_SECRET_KEY")
User_name = os.getenv("USERNAME")
service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
MB_URL = os.getenv("METABASE_URL")
BASE_QUERY_URL = os.getenv("MOM_FUNNEL_BASE_QUERY")
RFD_QUERY_URL = os.getenv("MOM_FUNNEL_RFD_QUERY")
SAK = os.getenv("SHEET_ACCESS_KEY")

if not sec or not service_account_json:
    raise ValueError("❌ Missing environment variables. Check GitHub secrets.")

# -------------------- GOOGLE AUTH --------------------
service_info = json.loads(service_account_json)

creds = Credentials.from_service_account_info(
    service_info,
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

gc = gspread.authorize(creds)

# -------------------- METABASE LOGIN --------------------
print("🔐 Creating Metabase session...")

res = requests.post(
    MB_URL,
    headers={"Content-Type": "application/json"},
    json={"username": User_name, "password": sec},
    timeout=60
)

res.raise_for_status()
token = res.json()['id']

METABASE_HEADERS = {
    "Content-Type": "application/json",
    "X-Metabase-Session": token
}

print("✅ Metabase session created")

# -------------------- FETCH WITH RETRY --------------------
def fetch_with_retry(url, headers, retries=5):
    for attempt in range(1, retries + 1):
        try:
            response = requests.post(url, headers=headers, timeout=180)
            response.raise_for_status()
            return response
        except Exception as e:
            wait_time = 10 * attempt
            print(f"[Metabase] Attempt {attempt} failed: {e}")
            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise

# -------------------- SANITIZE DATAFRAME --------------------
def sanitize_df(df):
    df.replace([np.inf, -np.inf], None, inplace=True)
    df = df.fillna("")
    df = df.astype(str)
    df = df.replace("None", "")
    return df

# -------------------- SAFE SHEET UPDATE --------------------
def safe_update_sheet(worksheet, df, clear_range, retries=5):
    print(f"🔄 Updating worksheet: {worksheet.title}")

    for attempt in range(1, retries + 1):
        try:
            rows = len(df) + 1
            cols = len(df.columns)

            # Clear only specified range
            worksheet.batch_clear([clear_range])

            # Prepare values
            header = df.columns.tolist()
            data_rows = df.values.tolist()

            # Sanitize after tolist()
            def sanitize_row(row):
                return [str(v) if v is not None else "" for v in row]

            data_rows = [sanitize_row(row) for row in data_rows]
            values = [header] + data_rows

            worksheet.update(
                f"A1:{chr(64 + cols)}{rows}",
                value_input_option="USER_ENTERED"
            )

            print(f"✅ Sheet updated successfully: {worksheet.title}")
            return True

        except Exception as e:
            wait_time = 15 * attempt
            print(f"[Sheets] Attempt {attempt} failed: {e}")
            if attempt < retries:
                print(f"⏳ Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise

# -------------------- COLUMN ORDERS --------------------
BASE_COLS = [
    'month_bucket', 'sales_user_email', 'prospect_id', 'prospect_email',
    'lead_created_on', 'assignment_ts', 'enrollment_ts', 'first_touch_ts',
    'utm_source', 'inbound_source', 'first_touch_channel', 'is_prospect',
    'is_rejected', 'test_taken', 'session_done', 'rfd', 'total_dials',
    'total_connects', 'dialled_flag', 'connect_flag', 'true_churn'
]

RFD_COLS = [
    'prospect_id', 'prospect_email', 'sales_user_email', 'lead_created_on',
    'assignment_ts', 'enrollment_ts', 'first_touch_ts', 'utm_source',
    'inbound_source', 'rfd_cohort_type', 'first_touch_channel'
]

# -------------------- CONNECT TO SHEET --------------------
print("🔗 Connecting to Google Sheets...")
sheet = gc.open_by_key(SAK)

ws_base = sheet.worksheet("Base")
ws_rfd = sheet.worksheet("RFD")

# -------------------- QUERY 1: BASE QUERY --------------------
print("📥 Fetching Base Query from Metabase...")
response_base = fetch_with_retry(BASE_QUERY_URL, METABASE_HEADERS)
df_base = pd.DataFrame(response_base.json())

if df_base.empty:
    print("⚠️ WARNING: Base Query returned empty dataset.")
else:
    print(f"📊 Base Query rows fetched: {len(df_base)}")

    missing_base = [col for col in BASE_COLS if col not in df_base.columns]
    if missing_base:
        raise ValueError(f"❌ Missing columns in Base query: {missing_base}")

    df_base = df_base[BASE_COLS]
    df_base = sanitize_df(df_base)
    print("⬆️ Updating Base tab...")
    safe_update_sheet(ws_base, df_base, "A:U")

# -------------------- QUERY 2: RFD QUERY --------------------
print("📥 Fetching RFD Query from Metabase...")
response_rfd = fetch_with_retry(RFD_QUERY_URL, METABASE_HEADERS)
df_rfd = pd.DataFrame(response_rfd.json())

if df_rfd.empty:
    print("⚠️ WARNING: RFD Query returned empty dataset.")
else:
    print(f"📊 RFD Query rows fetched: {len(df_rfd)}")

    missing_rfd = [col for col in RFD_COLS if col not in df_rfd.columns]
    if missing_rfd:
        raise ValueError(f"❌ Missing columns in RFD query: {missing_rfd}")

    df_rfd = df_rfd[RFD_COLS]
    df_rfd = sanitize_df(df_rfd)
    print("⬆️ Updating RFD tab...")
    safe_update_sheet(ws_rfd, df_rfd, "A:K")

# -------------------- TIMER SUMMARY --------------------
end_time = time.time()
elapsed = end_time - start_time
mins, secs = divmod(elapsed, 60)

print(f"⏱ Total execution time: {int(mins)}m {int(secs)}s")
print("🎯 MoM Funnel Automation Completed Successfully!")
