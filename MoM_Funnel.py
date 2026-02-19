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

    def clean_value(x):
        if x is None:
            return ""
        if isinstance(x, float):
            if math.isnan(x) or math.isinf(x):
                return ""
        return x

    df = df.apply(lambda col: col.map(clean_value))
    return df

# -------------------- SAFE SHEET UPDATE --------------------
def safe_update_sheet(worksheet, df, retries=5):
    print(f"🔄 Updating worksheet: {worksheet.title}")

    for attempt in range(1, retries + 1):
        try:
            rows = len(df) + 1
            cols = len(df.columns)

            # Clear the sheet
            worksheet.clear()

            # Prepare values
            header = df.columns.tolist()
            data_rows = df.values.tolist()

            # Sanitize row by row after tolist()
            def sanitize_row(row):
                return [
                    None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v
                    for v in row
                ]

            data_rows = [sanitize_row(row) for row in data_rows]
            values = [header] + data_rows

            worksheet.update(
                f"A1:{chr(64 + cols)}{rows}",
                values
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

# -------------------- CONNECT TO SHEET --------------------
print("🔗 Connecting to Google Sheets...")
sheet = gc.open_by_key(SAK)

ws_base = sheet.worksheet("Feb Base")
ws_rfd = sheet.worksheet("RFDs")

# -------------------- QUERY 1: BASE QUERY --------------------
print("📥 Fetching Base Query from Metabase...")
response_base = fetch_with_retry(BASE_QUERY_URL, METABASE_HEADERS)
df_base = pd.DataFrame(response_base.json())

if df_base.empty:
    print("⚠️ WARNING: Base Query returned empty dataset.")
else:
    print(f"📊 Base Query rows fetched: {len(df_base)}")
    df_base = sanitize_df(df_base)
    print("⬆️ Updating Feb Base tab...")
    safe_update_sheet(ws_base, df_base)

# -------------------- QUERY 2: RFD QUERY --------------------
print("📥 Fetching RFD Query from Metabase...")
response_rfd = fetch_with_retry(RFD_QUERY_URL, METABASE_HEADERS)
df_rfd = pd.DataFrame(response_rfd.json())

if df_rfd.empty:
    print("⚠️ WARNING: RFD Query returned empty dataset.")
else:
    print(f"📊 RFD Query rows fetched: {len(df_rfd)}")
    df_rfd = sanitize_df(df_rfd)
    print("⬆️ Updating RFDs tab...")
    safe_update_sheet(ws_rfd, df_rfd)

# -------------------- TIMER SUMMARY --------------------
end_time = time.time()
elapsed = end_time - start_time
mins, secs = divmod(elapsed, 60)

print(f"⏱ Total execution time: {int(mins)}m {int(secs)}s")
print("🎯 MoM Funnel Automation Completed Successfully!")
```

---
