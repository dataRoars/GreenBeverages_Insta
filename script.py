import requests
import pandas as pd
from datetime import datetime
import os
from google.cloud import bigquery

import json

# Create credentials file from GitHub secret
with open("credentials.json", "w") as f:
    f.write(os.getenv("GCP_CREDENTIALS"))

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials.json"

ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
IG_USER_ID = "17841404064297182"

PROJECT_ID = "green-beverages-492510"
DATASET = "social_media"
TABLE_POST = "instagram_posts"
TABLE_ACCOUNT = "instagram_account"

BASE_URL = "https://graph.facebook.com/v19.0"

# -----------------------------
# 1. ACCOUNT DATA (APPEND)
# -----------------------------
def get_account_data():
    url = f"{BASE_URL}/{IG_USER_ID}/insights"
    params = {
        "metric": "reach",
        "period": "day",
        "access_token": ACCESS_TOKEN
    }

    res = requests.get(url, params=params).json()

    data = []
    today = datetime.utcnow().date()

    for metric in res.get("data", []):
        for val in metric.get("values", []):
            data.append({
                "date": val["end_time"][:10],
                "reach": val["value"],
                "load_date": str(today)
            })

    return pd.DataFrame(data)


# -----------------------------
# 2. POSTS DATA
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

        for post in res.get("data", []):
            posts.append({
                "post_id": post["id"],
                "date": post["timestamp"][:10],
                "media_type": post.get("media_type"),
                "caption": post.get("caption"),
                "likes": post.get("like_count"),
                "comments": post.get("comments_count"),
                "load_date": str(datetime.utcnow().date())
            })

        url = res.get("paging", {}).get("next")
        params = None

    return pd.DataFrame(posts)


# -----------------------------
# 3. PUSH TO BIGQUERY
# -----------------------------
def load_to_bigquery(df, table):
    client = bigquery.Client()

    job = client.load_table_from_dataframe(
        df,
        f"{PROJECT_ID}.{DATASET}.{table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND"
        )
    )

    job.result()


# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("Fetching account data...")
    account_df = get_account_data()

    print("Fetching posts...")
    post_df = get_posts()

    print("Uploading to BigQuery...")
    load_to_bigquery(account_df, TABLE_ACCOUNT)
    load_to_bigquery(post_df, TABLE_POST)

    print("Done ✅")
