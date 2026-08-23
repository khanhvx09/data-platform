import os
import numpy as np
import pandas as pd
import streamlit as st
from databricks import sql
from openai import OpenAI
import json
from dotenv import load_dotenv

load_dotenv()

MODEL = "gpt-4o-mini"

FORBIDDEN_WORDS = ['drop', 'delete', 'truncate', 'alter', 'update', 'insert', 'create', 'replace', 'grant', 'revoke']

EXAMPLE_QUESTIONS = [
    "Top 10 cities by GMV",
    "Which cuisin has the most orders?",
    "Average delivery time by city, worst first",
    "Cancel rate by payment method"
]

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SCHEMA = """
Tables available (Databricks, catalog zomato, schema gold). Use bare table names, no catalog or schema prefix.

fct_orders(order_id, order_date, customer_id, restaurant_id,
           payment_method, order_status, is_delivered, sales_amount, discount,
           delivery_fee, gst, customer_rating, delivery_time_min)
dim_restaurant(restaurant_id, restaurant_name, city, cuisine, rating, cost_for_two)
dim_customer(customer_id, customer_name, age, age_segment, gender)
mart_daily_city_revenune(order_date, city, orders, delivered_orders, cancel_rate, gmv, aov)
mart_restaurant_performance(restaurant_id, restaurant_name, city, cuisine,
                            orders, revenue, avg_customer_rating, avg_delivery_min)
mart_delivery_sla(city, order_hour, delivered_orders, p50, p90)


Note: gmv means delivered revenue. p50/p90 in mart_delivery_sla are delivery_time_min percentiles (minutes).
Prefer the mart_ tables when they fit the question.
"""

SYSTEM_PROMPT = f"""
You are a Databricks SQL expert. Write ONE SELECT query that answers the question.

Rules:
- SELECT queries only, never modify data.
- Use bare table names (fct_orders, not zomato.gold.fct_orders).
- Add a LIMIT of 100 or less, unless the question asks for a single total.
- Reply as JSON in this exact format: {{"sql": "your query here"}}

{SCHEMA}
"""
 


@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        token=os.getenv("DATABRICKS_TOKEN"),
        catalog="zomato",
        schema="gold"
    )


def generate_sql(question):
    response = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question}
        ]
    )
    answer = response.choices[0].message.content
    generated_sql = json.loads(answer)["sql"]

    generated_sql = generated_sql.replace("zomato.gold.", "").replace("zomato.", "")
    return generated_sql.strip().rstrip(";")


def is_safe(sql):
    lowered = sql.lower()

    if not lowered.startswith("select") and not lowered.startswith("with"):
        return False

    for word in FORBIDDEN_WORDS:
        if word in lowered:
            return False

    return True

def run_query(query):
    conn = get_connection()
    cursor = conn.cursor()
    return cursor.execute(query).fetchall_arrow().to_pandas()


st.title("Chat with your Zomato Data")
st.caption(f"Ask in English, {MODEL} writes the SQL, Databricks runs it")

with st.sidebar:
    st.header("Example Questions")
    for q in EXAMPLE_QUESTIONS:
        st.markdown(f" - {q}")

question = st.text_input("Enter your question here", 
                         placeholder="e.g. Top 10 restaurants by revenune in Banglore")


if question:
    sql_query = generate_sql(question)
    st.code(sql_query, language="sql")

    if not is_safe(sql_query):
        st.error("The generated SQL is not safe to run. Please modify your question.")

    else:
        try:
            df = run_query(sql_query)
            st.success(f"{len(df)} rows returned")
            st.dataframe(df, hide_index=True)

            if len(df.columns) == 2 and pd.api.types.is_numeric_dtype(df.iloc[:, 1]):
                st.bar_chart(df, x=df.columns[0], y=df.columns[1])

        except Exception as e:
            st.error(f"Error running query: {e}")