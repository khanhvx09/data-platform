import os
import json
from databricks import sql
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

MODEL = "gpt-4o-mini"

SAMPLE_N = 5
TOPICS = ["food quality", "delivery", "pricing", "service", "packaging", "other"]

CATALOG = os.getenv("DATABRICKS_CATALOG", "zomato")
SOURCE_TABLE = f"{CATALOG}.silver.silver_reviews"
OUTPUT_SCHEMA = f"{CATALOG}.gold"
OUTPUT_TABLE = f"{OUTPUT_SCHEMA}.review_enriched"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = f"""
You classify customer reviews for a food delivery app.

For the review you are given, return:
- sentiment_label: positive, negative, or neutral
- sentiment_score: a number between -1.0 and 1.0
- topic: one of {TOPICS}
- key_issue: a short phrase of 6 words or less that describes the main issue in the review, if any. If there is no issue, return null

Reply as JSON in this exact format:
{{
    "sentiment_label": "<sentiment_label>",
    "sentiment_score": <sentiment_score>,
    "topic": "<topic>",
    "key_issue": "<key_issue>"}}
"""

def get_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    )

def create_output_table(cursor):
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {OUTPUT_SCHEMA}")
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {OUTPUT_TABLE} (
            review_id BIGINT,
            sentiment_label STRING,
            sentiment_score DOUBLE,
            topic STRING,
            key_issue STRING,
            model STRING,
            enriched_at TIMESTAMP
        ) USING DELTA
    """)

def get_reviews_to_enrich(cursor):
    cursor.execute(f"""
        SELECT review_id, comment
        FROM {SOURCE_TABLE}
        WHERE review_id NOT IN (SELECT review_id FROM {OUTPUT_TABLE})
        LIMIT {SAMPLE_N}
    """)
    return cursor.fetchall()

def classify_review(comment):
    response = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": comment}
        ]
    )
    answer = response.choices[0].message.content
    return json.loads(answer)

def save_results(cursor, results):
    """Insert all the enriched rows into Databricks in one go."""
    print(f"Saving {len(results)} enriched reviews to Databricks...")
    cursor.executemany(
        f"""
        INSERT INTO {OUTPUT_TABLE}
            (review_id, sentiment_label, sentiment_score, topic, key_issue, model, enriched_at)
        VALUES (?, ?, ?, ?, ?, ?, current_timestamp())
        """,
        results,
    )


def main():
    conn = get_connection()
    cursor = conn.cursor()
    create_output_table(cursor)
    reviews = get_reviews_to_enrich(cursor)

    if len(reviews) == 0:
        print("No new reviews to enrich.")
        return

    print(f"Enriching {len(reviews)} reviews...")

    results = []
    for review_id, comment in reviews:
        print(f"Classifying review {review_id}: {comment}")
        try:
            labels = classify_review(comment)
            print(f"Labels for review {review_id}: {labels}")
            results.append((
                review_id,
                labels["sentiment_label"],
                labels["sentiment_score"],
                labels["topic"],
                labels["key_issue"],
                MODEL
            ))
        except Exception as e:
            print(f"Error occurred while classifying review {review_id}: {e}")

    save_results(cursor, results)
    print(f"Saved {len(results)} enriched reviews to Databricks.")
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
