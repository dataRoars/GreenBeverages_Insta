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
# 1. ACCOUNT LEVEL DATA
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
    today = datetime.utcnow().date()

    for metric in res.get("data", []):
        name = metric.get("name")

        for val in metric.get("values", []):
            data.append({
                "date": val["end_time"][:10],
                "metric": name,
                "value": val["value"],
                "load_date": str(today)
            })

    df = pd.DataFrame(data)
    print("Account rows:", len(df))
    return df


# -----------------------------
# 2. GET ALL POSTS
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
# 3. POST DATA (NO EXTRA API CALL NEEDED)
# -----------------------------
def build_post_data(posts):
    data = []
    today = datetime.utcnow().date()

    for post in posts:
        data.append({
            "post_id": post["id"],
            "date": post["timestamp"][:10],
            "media_type": post.get("media_type"),
            "caption": post.get("caption"),
            "likes": post.get("like_count"),
            "comments": post.get("comments_count"),
            "load_date": str(today)
        })

    df = pd.DataFrame(data)
    print("Post rows:", len(df))
    return df


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
# LOAD TO BIGQUERY
# -----------------------------
def load_to_bigquery(df, table):
    if df.empty:
        print(f"⚠️ No data for {table}, skipping...")
        return

    client = get_bq_client()

    job = client.load_table_from_dataframe(
        df,
        f"{PROJECT_ID}.{DATASET}.{table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )

    job.result()
    print(f"✅ Loaded {len(df)} rows into {table}")

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("🚀 Starting pipeline...")

    print("Fetching account data...")
    account_df = get_account_data()

    print("Fetching posts...")
    posts = get_posts()

    print("Building post data...")
    post_df = build_post_data(posts)

    print("Uploading to BigQuery...")
    load_to_bigquery(account_df, TABLE_ACCOUNT)
    load_to_bigquery(post_df, TABLE_POST)

    print("✅ Done!")
