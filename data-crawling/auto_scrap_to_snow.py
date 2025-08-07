import requests
from bs4 import BeautifulSoup
import snowflake.connector
from datetime import datetime

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = 'oh29385.ap-southeast-3.aws'
SNOWFLAKE_USER = 'ageunghidayat'
SNOWFLAKE_PASSWORD = 'P@ssvv0rd'
SNOWFLAKE_DATABASE = 'SCRAPPING'
SNOWFLAKE_SCHEMA = 'public'
SNOWFLAKE_WAREHOUSE = 'compute_wh'
SNOWFLAKE_TABLE = 'product_scrapping'
SNOWFLAKE_SPAM_TABLE = 'spam_merchant'

# Replace with your Telegram bot token
TELEGRAM_BOT_TOKEN = '7084521954:AAGNneq4ilFjE8kIw52NXXD0BDIiyLtYlC8'
# Replace with your chat ID
TELEGRAM_CHAT_ID = '1167165295'

# URL of the Tokopedia search page
url = "https://www.tokopedia.com/search?navsource=&ob=9&pmax=3300000&pmin=1000000&search_id=20240728072105DD1E77EEDCE5240F3FGI&srp_component_id=04.06.00.00&srp_page_id=&srp_page_title=&st=&q=nintendo%20switch"

# Headers to mimic a browser
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
}

# Function to fetch page content
def fetch_page_content(url):
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.content
    except requests.exceptions.RequestException as e:
        print(f"Error fetching the page: {e}")
        return None

# Function to extract product details
def extract_products(soup):
    products = soup.find_all("div", class_="css-5wh65g")[:10]  # Get only top 10 products
    product_data = []
    for product in products:
        title_element = product.find("div", class_="VKNwBTYQmj8+cxNrCQBD6g==")
        title = title_element.text if title_element else "No title"

        price_element = product.find("div", class_="ELhJqP-Bfiud3i5eBR8NWg==")
        price = price_element.text if price_element else "No price"

        url_element = product.find('a', href=True)
        url = url_element["href"] if url_element else "No URL"

        merchant_element = product.find("div", class_="_4iyO0jMqM71An9gZaTzQig==")
        merchant = merchant_element.text if merchant_element else "No merchant"

        product_data.append((title, price, url, merchant, datetime.now()))

    return product_data

# Function to store data in Snowflake and send new data to Telegram
def store_in_snowflake(data):
    new_data = []
    merchant_count = {}
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE
    )
    cursor = conn.cursor()

    # Create tables if they don't exist
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        TITLE STRING,
        PRICE STRING,
        URL STRING,
        MERCHANT STRING,
        INSERTED_AT TIMESTAMP
    )
    """
    cursor.execute(create_table_query)

    create_spam_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_SPAM_TABLE} (
        MERCHANT STRING,
        FLAGGED_AT TIMESTAMP
    )
    """
    cursor.execute(create_spam_table_query)

    # Fetch existing product titles and merchants from Snowflake
    fetch_existing_titles_query = f"SELECT TITLE FROM {SNOWFLAKE_TABLE}"
    cursor.execute(fetch_existing_titles_query)
    existing_titles = {row[0] for row in cursor.fetchall()}

    fetch_existing_merchants_query = f"""
    SELECT MERCHANT, COUNT(*) 
    FROM {SNOWFLAKE_TABLE} 
    WHERE INSERTED_AT >= CURRENT_DATE
    GROUP BY MERCHANT
    """
    cursor.execute(fetch_existing_merchants_query)
    existing_merchant_counts = {row[0]: row[1] for row in cursor.fetchall()}

    # Fetch spam merchants
    fetch_spam_merchants_query = f"SELECT MERCHANT FROM {SNOWFLAKE_SPAM_TABLE}"
    cursor.execute(fetch_spam_merchants_query)
    spam_merchants = {row[0] for row in cursor.fetchall()}

    # Insert data and collect new rows
    for product in data:
        merchant = product[3]
        if product[0] not in existing_titles and merchant not in spam_merchants:
            insert_query = f"""
            INSERT INTO {SNOWFLAKE_TABLE} (TITLE, PRICE, URL, MERCHANT, INSERTED_AT)
            VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, product)
            new_data.append(product)

            # Update merchant count
            if merchant in merchant_count:
                merchant_count[merchant] += 1
            else:
                merchant_count[merchant] = existing_merchant_counts.get(merchant, 0) + 1

            # Check if merchant is spam
            if merchant_count[merchant] > 3:
                insert_spam_query = f"""
                INSERT INTO {SNOWFLAKE_SPAM_TABLE} (MERCHANT, FLAGGED_AT)
                VALUES (%s, %s)
                """
                cursor.execute(insert_spam_query, (merchant, datetime.now()))

    conn.commit()
    cursor.close()
    conn.close()

    # Send new data to Telegram
    if new_data:
        send_to_telegram(new_data)

# Function to send data to Telegram
def send_to_telegram(data):
    for product in data:
        message = (
            f"New Product:\n"
            f"Title: {product[0]}\n"
            f"Price: {product[1]}\n"
            f"URL: {product[2]}\n"
            f"Merchant: {product[3]}\n"
            f"Inserted At: {product[4]}"
        )
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message
        }
        try:
            response = requests.post(url, data=payload)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error sending message to Telegram: {e}")

# Main script
content = fetch_page_content(url)
if content:
    soup = BeautifulSoup(content, "html.parser")
    products = extract_products(soup)
    store_in_snowflake(products)
else:
    print("Failed to retrieve the page content.")

