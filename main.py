import pandas as pd
import sqlite3
import os
import google.genai.types as types
from google.genai import Client
import dotenv
import json
import time

# Connect to API and Version
dotenv.load_dotenv()
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GEMINI_MODEL = "gemini-2.5-flash-lite"
# Size of request to gemini
Batch_size = 100

client = None

# Check Error API
if GOOGLE_API_KEY:
    try:
        client = Client(api_key=GOOGLE_API_KEY)

    except Exception as e:
        print(f"CRITICAL ERROR: Failed to initialise Gemini client. Error: {e}")



# JSON plan
review_schema = types.Schema(
    type=types.Type.ARRAY,
    description="Array of processed reviews",
    items=types.Schema(
        type=types.Type.OBJECT,
        properties={
            "id": types.Schema(type=types.Type.INTEGER, description="Original Review ID"),
            "category": types.Schema(type=types.Type.STRING, description="Main category (Design, Effectiveness, Quality, Price, Usability, Customer Service, Delivery, Other."),
            "sentiment": types.Schema(type=types.Type.STRING, description="Sentiment (Positive, Negative, Neutral)"),

        },
        required=["id", "category", "sentiment"]
    )
)

# Convert CSV to SQL
def load_csv_to_sqlite():
    conn = sqlite3.connect('reviews.db')
    c = conn.cursor()
    df = pd.read_csv("reviews_kaggle.csv", low_memory=False)

    c.execute('''
              CREATE TABLE IF NOT EXISTS reviews
              (

                  id              INTEGER PRIMARY KEY AUTOINCREMENT,
                  author_id       INTEGER,
                  brand_name      TEXT,
                  submission_time INTEGER,
                  rating          INTEGER,
                  review_title    TEXT,
                  review_text     TEXT,
                  product_name    TEXT,
                  category        TEXT
              );
              ''')
    for _, row in df.iterrows():
        if row['brand_name'] == "FOREO":
            c.execute(
                "INSERT INTO reviews (author_id,brand_name,submission_time,rating,review_title,review_text, product_name, category) VALUES (?,?, ?, ?, ? ,? , ?, ?)",
                (
                    row['author_id'],
                    row['brand_name'],
                    row['submission_time'],
                    row['rating'],
                    row['review_title'],
                    row['review_text'],
                    row['product_name'],
                    None)
            )
    conn.commit()

    df = pd.read_sql_query("SELECT brand_name,product_name,review_text FROM reviews LIMIT 10", conn)
    print(df)
    conn.close()

# load_csv_to_sqlite()
# Create analysis DB
def setup_analyzed_db():
    conn = sqlite3.connect('sentiment_analysis.db')
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS sentiment_analysis(
            id INTEGER PRIMARY KEY, 
            author_id INTEGER,
            category TEXT,
            sentiment TEXT,
            product_name TEXT,
            review_text TEXT,
            submission_time INTEGER
        )
    ''')
    conn.commit()
    conn.close()
# Minimize tokens because of joins two DB
def fetch_reviews_batch():
    conn_row = sqlite3.connect('reviews.db')


    conn_analyzed = sqlite3.connect('sentiment_analysis.db')
    processed_ids = pd.read_sql_query("SELECT id FROM sentiment_analysis", conn_analyzed)['id'].tolist()
    conn_analyzed.close()
    if not processed_ids:
        ids_filter = " (1=1) "
    else:
        ids_list_str = ', '.join(map(str, processed_ids))
        ids_filter = f" id NOT IN ({ids_list_str}) "
    query = f"""
        SELECT id,author_id, review_text, product_name,submission_time FROM reviews 
        WHERE {ids_filter}
        LIMIT {Batch_size}
        """
    df_batch = pd.read_sql_query(query, conn_row)
    conn_row.close()

    return df_batch.to_dict('records')

# Saving proces after response from gemini
def save_analyzed_data(reviews_batch, analyzed_data):

    if not analyzed_data:
        return
    df_analyzed = pd.DataFrame(analyzed_data)
    df_raw = pd.DataFrame(reviews_batch)
    df_merged = df_analyzed.merge(
        df_raw[['id', 'product_name', 'author_id','submission_time','review_text']],
        on='id',
        how='left'
    )
    conn = sqlite3.connect('sentiment_analysis.db')
    df_merged[['id', 'sentiment', 'category', 'product_name', 'author_id','submission_time','review_text']].to_sql(
        'sentiment_analysis',
        conn,
        if_exists='append',
        index=False,
        method='multi')
    conn.close()
    print("Successfully saved")


# Main promt request
def gemini_sentiment_analysis(reviews_batch):
    if not reviews_batch:
        return None


    promt = f'''
    You are an expert in analyzing customer reviews of FOREO products.
    Your task is to analyze EVERY review from the list below.
    
    Classification rules:
    1. **Sentiment:** Use: Positive, Negative, Neutral, Mixed (if there are strong pros and cons).
    2. **Category:** Use strictly one of the following: Effectiveness, Quality, Price, Usability, Customer Service, Delivery, Design, Other.
    3. **Response format:** Return a JSON array including ‘id’, ‘category’, and ‘sentiment’ for each review.

LIST OF REVIEWS FOR ANALYSIS (Batch Size: {len(reviews_batch)}):
'''
    for item in reviews_batch:
        # Добавляем все ключевые поля в промпт для анализа
        promt += f"ID: {item['id']} | PRODUCT: {item['product_name']} | AUTHOR_ID: {item['author_id']} | TEXT: {item['review_text']} | SUBMISSION_TIME: {item['submission_time']}\n"

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=promt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=review_schema,
            ),
        )
        return json.loads(response.text)

    except Exception as e:
        print(f"Error API: {e}")
        return None

# Run program
def run_analysis_pipeline():
    if not client:
        print("Client is not connected")
        return
    setup_analyzed_db()
    while True:
        reviews_batch = fetch_reviews_batch()

        if not reviews_batch:
            print("All reviews have been fetched.Time sleeping... 60s.")
            time.sleep(60)
            continue

        print(f"Start analysis {len(reviews_batch)}")

        analyzed_data = gemini_sentiment_analysis(reviews_batch)

        if analyzed_data:
            # insertion of results into the database
            save_analyzed_data(reviews_batch,analyzed_data)
            print(f"Successfully saved {len(analyzed_data)} ")

            # Pause to comply with RPM limit
            time.sleep(5)
        else:
            # Long pause if the API returned an error or the limit was exceeded
            print("Error API or Limits.Time sleeping... 30s.")
            time.sleep(30)


#load_csv_to_sqlite()
run_analysis_pipeline()

