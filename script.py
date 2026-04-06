import requests
import pandas as pd
from datetime import datetime
import os
from google.cloud import bigquery
import json
from google.oauth2 import service_account

# -----------------------------
# CONFIG
# -----------------------------
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
IG_USER_ID = "17841404064297182"

PROJECT_ID = "green-beverages-492510"
DATASET = "social_media"
TABLE_POST = "instagram_posts"
TABLE_ACCOUNT = "instagram_account"

BASE_URL = "https://graph.facebook.com/v19.0"

# -----------------------------
# BIGQUERY CLIENT
# -----------------------------
def get_bq_client():
    creds_json = os.getenv("GCP_CREDENTIALS")
    creds_dict = json.loads(creds_json)

    credentials = service_account.Credentials.from_service_account_info(creds_dict)

    return bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )

# -----------------------------
# GET EXISTING DATES (ACCOUNT)
# -----------------------------
def get_existing_dates(table):
    client = get_bq_client()

    query = f"""
        SELECT DISTINCT date 
        FROM `{PROJECT_ID}.{DATASET}.{table}`
    """

    try:
        result = client.query(query).result()
        existing_dates = set([str(row.date) for row in result])
        print("Existing dates in BQ:", existing_dates)
        return existing_dates
    except:
        print("Table not found or empty, first run.")
        return set()
# -----------------------------
# GET EXISTING POST IDS
# -----------------------------
def get_existing_post_ids():
    client = get_bq_client()

    query = f"""
        SELECT DISTINCT post_id 
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_POST}`
    """

    try:
        result = client.query(query).result()
        return set([row.post_id for row in result])
    except:
        print("Post table not found (first run)")
        return set()
# -----------------------------
# 1. ACCOUNT DATA
# -----------------------------
def get_account_data():
    url = f"{BASE_URL}/{IG_USER_ID}/insights"
    params = {
        "metric": "reach",
        "period": "day",
        "access_token": ACCESS_TOKEN
    }

    res = requests.get(url, params=params).json()
    print("ACCOUNT RESPONSE:", res)

    if "error" in res:
        print("ACCOUNT API ERROR:", res)
        return pd.DataFrame()

    data = []

    for metric in res.get("data", []):
        for val in metric.get("values", []):
            data.append({
                "date": val["end_time"][:10],
                "reach": val["value"]
            })

    df = pd.DataFrame(data)
    print("Account rows (raw):", len(df))
    return df

# -----------------------------
# 2. POSTS
# -----------------------------
def get_posts():
    url = f"{BASE_URL}/{IG_USER_ID}/media"
    params = {
        "fields": "id,caption,media_type,timestamp,like_count,comments_count",
        "access_token": ACCESS_TOKEN,
        "limit": 50
    }

    posts = []

    while url:
        res = requests.get(url, params=params).json()

        if "error" in res:
            print("POST API ERROR:", res)
            break

        for post in res.get("data", []):
            posts.append(post)

        url = res.get("paging", {}).get("next")
        params = None

    print("Total posts fetched:", len(posts))
    return posts

# -----------------------------
# BUILD POST DATA
# -----------------------------
def build_post_data(posts):
    data = []

    for post in posts:
        data.append({
            "post_id": post["id"],
            "date": post["timestamp"][:10],
            "media_type": post.get("media_type"),
            "caption": post.get("caption"),
            "likes": post.get("like_count"),
            "comments": post.get("comments_count")
        })

    df = pd.DataFrame(data)

    # ✅ Remove duplicates inside dataframe
    df = df.drop_duplicates(subset=["post_id"])

    print("Post rows (after dedupe):", len(df))
    return df

# -----------------------------
# LOAD TO BIGQUERY
# -----------------------------
def load_to_bigquery(df, table):
    if df.empty:
        print(f"⚠️ No data for {table}, skipping...")
        return

    client = get_bq_client()
    table_id = f"{PROJECT_ID}.{DATASET}.{table}"

    print(f"\nUploading to {table_id}")
    print("Rows:", len(df))

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )

    job.result()

    print("Rows loaded:", job.output_rows)

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("🚀 Starting pipeline...")

    # ACCOUNT DATA
    account_df = get_account_data()

    # ✅ REMOVE DUPLICATES FROM API
    account_df = account_df.drop_duplicates(subset=["date"])
    
    # ✅ REMOVE EXISTING POSTS FROM BIGQUERY
    existing_post_ids = get_existing_post_ids()
    post_df = post_df[~post_df["post_id"].isin(existing_post_ids)]

    print("Post rows (after BQ filter):", len(post_df))
    
    # ✅ FILTER EXISTING DATES FROM BIGQUERY
    existing_dates = get_existing_dates(TABLE_ACCOUNT)
    account_df = account_df[~account_df["date"].isin(existing_dates)]

    print("Account rows (final):", len(account_df))

    # POSTS DATA
    posts = get_posts()
    post_df = build_post_data(posts)

    # LOAD
    load_to_bigquery(account_df, TABLE_ACCOUNT)
    load_to_bigquery(post_df, TABLE_POST)

    print("✅ DONE")
