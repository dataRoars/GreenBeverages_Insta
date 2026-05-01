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
TABLE_DAILY = "instagram_daily_metrics"
TABLE_FOLLOWER = "instagram_followers"
TABLE_PROFILE = "instagram_profile"
TABLE_DEMO_GENDER = "instagram_demographics_gender"
TABLE_DEMO_COUNTRY = "instagram_demographics_country"
TABLE_DEMO_AGE = "instagram_demographics_age"
TABLE_POST_INSIGHTS = "instagram_post_insights"

BASE_URL = "https://graph.facebook.com/v25.0"

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
# GET EXISTING DATES
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
        print(f"Existing dates in {table}:", existing_dates)
        return existing_dates
    except:
        print(f"Table {table} not found or empty, first run.")
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
# GET EXISTING POST INSIGHT IDS
# -----------------------------
def get_existing_insight_post_ids():
    client = get_bq_client()
    query = f"""
        SELECT DISTINCT post_id 
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_POST_INSIGHTS}`
    """
    try:
        result = client.query(query).result()
        return set([row.post_id for row in result])
    except:
        print("Post insights table not found (first run)")
        return set()

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
# 1. ACCOUNT DATA (EXISTING)
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
# 2. GET ALL POSTS (EXISTING)
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
# BUILD POST DATA (EXISTING)
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
    df = df.drop_duplicates(subset=["post_id"])
    print("Post rows (after dedupe):", len(df))
    return df

# -----------------------------
# 3. NEW — ACCOUNT DAILY METRICS
# -----------------------------
def get_account_daily_metrics():
    url = f"{BASE_URL}/{IG_USER_ID}/insights"
    params = {
        "metric": "reach,website_clicks,profile_views,accounts_engaged,total_interactions,saves,follows_and_unfollows",
        "period": "day",
        "metric_type": "total_value",
        "access_token": ACCESS_TOKEN
    }
    res = requests.get(url, params=params).json()
    print("DAILY METRICS RESPONSE:", res)

    if "error" in res:
        print("DAILY METRICS ERROR:", res)
        return pd.DataFrame()

    today = str(datetime.utcnow().date())
    row = {"date": today}

    for metric in res.get("data", []):
        name = metric.get("name")

        if name == "follows_and_unfollows":
            breakdowns = metric.get("total_value", {}).get("breakdowns", [])
            follows = 0
            unfollows = 0
            for b in breakdowns:
                for r in b.get("results", []):
                    if r.get("dimension_values") == ["FOLLOW"]:
                        follows = r.get("value", 0)
                    elif r.get("dimension_values") == ["UNFOLLOW"]:
                        unfollows = r.get("value", 0)
            row["follows"] = follows
            row["unfollows"] = unfollows
        else:
            value = metric.get("total_value", {}).get("value", 0)
            row[name] = value

    df = pd.DataFrame([row])
    print("Daily metrics row:", df)
    return df

# -----------------------------
# 4. NEW — FOLLOWER COUNT
# -----------------------------
def get_follower_count():
    url = f"{BASE_URL}/{IG_USER_ID}/insights"
    params = {
        "metric": "follower_count",
        "period": "day",
        "access_token": ACCESS_TOKEN
    }
    res = requests.get(url, params=params).json()
    print("FOLLOWER RESPONSE:", res)

    if "error" in res:
        print("FOLLOWER ERROR:", res)
        return pd.DataFrame()

    data = []
    for metric in res.get("data", []):
        for val in metric.get("values", []):
            data.append({
                "date": val["end_time"][:10],
                "follower_count": val["value"]
            })

    df = pd.DataFrame(data)
    df = df.drop_duplicates(subset=["date"])
    print("Follower rows:", len(df))
    return df

# -----------------------------
# 5. NEW — PROFILE SNAPSHOT
# -----------------------------
def get_profile_snapshot():
    url = f"{BASE_URL}/{IG_USER_ID}"
    params = {
        "fields": "followers_count,media_count,name,website",
        "access_token": ACCESS_TOKEN
    }
    res = requests.get(url, params=params).json()
    print("PROFILE RESPONSE:", res)

    if "error" in res:
        print("PROFILE ERROR:", res)
        return pd.DataFrame()

    today = str(datetime.utcnow().date())
    df = pd.DataFrame([{
        "date": today,
        "followers_count": res.get("followers_count"),
        "media_count": res.get("media_count"),
        "name": res.get("name"),
        "website": res.get("website")
    }])
    print("Profile snapshot:", df)
    return df

# -----------------------------
# 6. NEW — DEMOGRAPHICS
# -----------------------------
def get_demographics(breakdown):
    url = f"{BASE_URL}/{IG_USER_ID}/insights"
    params = {
        "metric": "follower_demographics",
        "period": "lifetime",
        "metric_type": "total_value",
        "breakdown": breakdown,
        "access_token": ACCESS_TOKEN
    }
    res = requests.get(url, params=params).json()
    print(f"DEMOGRAPHICS ({breakdown}) RESPONSE:", res)

    if "error" in res:
        print(f"DEMOGRAPHICS ({breakdown}) ERROR:", res)
        return pd.DataFrame()

    today = str(datetime.utcnow().date())
    data = []

    for metric in res.get("data", []):
        for b in metric.get("total_value", {}).get("breakdowns", []):
            for r in b.get("results", []):
                data.append({
                    "date": today,
                    "breakdown": breakdown,
                    "dimension": r.get("dimension_values", [None])[0],
                    "value": r.get("value", 0)
                })

    df = pd.DataFrame(data)
    print(f"Demographics ({breakdown}) rows:", len(df))
    return df

# -----------------------------
# 7. NEW — POST INSIGHTS
# -----------------------------
def get_post_insights(posts):
    existing_ids = get_existing_insight_post_ids()
    data = []

    for post in posts:
        post_id = post["id"]

        if post_id in existing_ids:
            continue

        url = f"{BASE_URL}/{post_id}/insights"
        params = {
            "metric": "reach,impressions,saved,likes,comments,shares",
            "access_token": ACCESS_TOKEN
        }

        res = requests.get(url, params=params).json()

        if "error" in res:
            print(f"POST INSIGHT ERROR for {post_id}:", res)
            continue

        row = {
            "post_id": post_id,
            "date": post["timestamp"][:10],
            "load_date": str(datetime.utcnow().date())
        }

        for metric in res.get("data", []):
            name = metric.get("name")
            values = metric.get("values", [])
            if values:
                row[name] = values[0].get("value", 0)

        data.append(row)

    df = pd.DataFrame(data)
    print("Post insights rows:", len(df))
    return df

# -----------------------------
# MAIN
# -----------------------------
if __name__ == "__main__":
    print("🚀 Starting pipeline...")

    # -------------------------
    # EXISTING FLOW (UNTOUCHED)
    # -------------------------
    print("\n📥 Fetching account data...")
    account_df = get_account_data()
    account_df = account_df.drop_duplicates(subset=["date"])
    existing_dates = get_existing_dates(TABLE_ACCOUNT)
    account_df = account_df[~account_df["date"].isin(existing_dates)]
    print("Account rows (final):", len(account_df))

    print("\n📥 Fetching posts...")
    posts = get_posts()
    post_df = build_post_data(posts)
    existing_post_ids = get_existing_post_ids()
    post_df = post_df[~post_df["post_id"].isin(existing_post_ids)]
    print("Post rows (after BQ filter):", len(post_df))

    load_to_bigquery(account_df, TABLE_ACCOUNT)
    load_to_bigquery(post_df, TABLE_POST)

    # -------------------------
    # NEW FLOW
    # -------------------------

    # Daily metrics
    print("\n📊 Fetching daily metrics...")
    daily_df = get_account_daily_metrics()
    daily_df = daily_df.drop_duplicates(subset=["date"])
    existing_daily_dates = get_existing_dates(TABLE_DAILY)
    daily_df = daily_df[~daily_df["date"].isin(existing_daily_dates)]
    print("Daily metrics rows (final):", len(daily_df))
    load_to_bigquery(daily_df, TABLE_DAILY)

    # Follower count
    print("\n👥 Fetching follower count...")
    follower_df = get_follower_count()
    follower_df = follower_df.drop_duplicates(subset=["date"])
    existing_follower_dates = get_existing_dates(TABLE_FOLLOWER)
    follower_df = follower_df[~follower_df["date"].isin(existing_follower_dates)]
    print("Follower rows (final):", len(follower_df))
    load_to_bigquery(follower_df, TABLE_FOLLOWER)

    # Profile snapshot
    print("\n📸 Fetching profile snapshot...")
    profile_df = get_profile_snapshot()
    profile_df = profile_df.drop_duplicates(subset=["date"])
    existing_profile_dates = get_existing_dates(TABLE_PROFILE)
    profile_df = profile_df[~profile_df["date"].isin(existing_profile_dates)]
    print("Profile rows (final):", len(profile_df))
    load_to_bigquery(profile_df, TABLE_PROFILE)

    # Demographics
    print("\n🌍 Fetching demographics...")
    existing_demo_dates = get_existing_dates(TABLE_DEMO_GENDER)

    gender_df = get_demographics("gender")
    gender_df = gender_df[~gender_df["date"].isin(existing_demo_dates)]
    print("Gender rows (final):", len(gender_df))
    load_to_bigquery(gender_df, TABLE_DEMO_GENDER)

    country_df = get_demographics("country")
    country_df = country_df[~country_df["date"].isin(existing_demo_dates)]
    print("Country rows (final):", len(country_df))
    load_to_bigquery(country_df, TABLE_DEMO_COUNTRY)

    age_df = get_demographics("age")
    age_df = age_df[~age_df["date"].isin(existing_demo_dates)]
    print("Age rows (final):", len(age_df))
    load_to_bigquery(age_df, TABLE_DEMO_AGE)

    # Post insights
    print("\n📝 Fetching post insights...")
    post_insights_df = get_post_insights(posts)
    print("Post insights rows (final):", len(post_insights_df))
    load_to_bigquery(post_insights_df, TABLE_POST_INSIGHTS)

    print("\n✅ DONE")
