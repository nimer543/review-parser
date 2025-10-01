import pandas as pd
import sqlite3
import google.generativeai as genai
import os
import dotenv

dotenv.load_dotenv()
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
os.environ["GRPC_VERBOSITY"] = "ERROR"
GEMINI_MODEL = "gemini-2.5-flash-lite"

try:
    # Initialise the Gemini client, wrap it in try/except in case of errors with the key
    genai.configure(api_key=GOOGLE_API_KEY)
except Exception as e:
    print(f"CRITICAL ERROR: Failed to initialise Gemini client. Is GEMINI_API_KEY set correctly? Error: {e}")

    client = None


conn = sqlite3.connect('reviews.db')
c = conn.cursor()
text_info = c.execute('SELECT review_text FROM reviews LIMIT 1').fetchone()
rating_info =  c.execute('SELECT rating FROM reviews LIMIT 1').fetchone()
product_info =  c.execute('SELECT product_name FROM reviews LIMIT 1').fetchone()


def gemini_sentiment_analysis():
    for text,rating  in zip(rating_info,text_info):
        prompt = f'''You are an AI that analyzes product reviews.  
    Your task is to classify each review into:  
    1. Main category  
    2. Sentiment (Positive, Negative, Neutral)  
    4.Provide the original product name in your response.
    3. Provide the original review text in your response.
    
    Review text: {text}  
    Review rating: {rating}  
    
    Categories: Design, Effectiveness, Price, Usability, Customer Service, Delivery, Other.  
    
    Output format (only this, no explanations):  
    Category: <one category name>  
    Sentiment: <Positive/Negative/Neutral>
    Product Name: <{product_info}>
    Text:<{text}>'''
        try:
            model = genai.GenerativeModel(GEMINI_MODEL)
            response = model.generate_content(prompt)
            sentiment = response.text.strip()
            print( sentiment)
        except Exception as e:
            print(f"Error during Gemini API call: {e}")
            return None

gemini_sentiment_analysis()
# gemini_sentiment_analysis()
def load_csv_to_sqlite():
    df = pd.read_csv("reviews_kaggle.csv",low_memory=False)



    c.execute('''
    CREATE TABLE IF NOT EXISTS reviews(
        
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        author_id INTEGER ,
        brand_name TEXT,
        submission_time INTEGER ,
        rating INTEGER ,
        review_title TEXT,
        review_text TEXT,
        product_name TEXT ,
        category TEXT);
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