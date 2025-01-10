

from bs4 import BeautifulSoup

# Dag defintion
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, dag
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

headers = {
    "Referer": 'https://www.amazon.com/',
    "Sec-Ch-Ua": "Not_A Brand",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "macOS",
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1.1 Safari/605.1.15'
}

@dag(schedule_interval='*/5 * * * *', default_args=default_args, catchup=False)
def fetch_and_store_amazon_books_etl():

    @task()
    def create_sql_tables():
        # Create a connection to the database
        postgres_hook = PostgresHook(postgres_conn_id='books_connection')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        # Create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS books (
            title VARCHAR(255),
            author VARCHAR(255),
            price VARCHAR(255),
            rating VARCHAR(255)
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()

    # Tasks: Extraction of amazon book data, Transformation of data, Loading of data
    @task()
    def extract_book_data(num_books: int = 10):

        # Fetch data from Amazon API
        url = f'https://www.amazon.sg/s?k=data+engineering+books'

        books = []
        seen_titles = set()

        page = 1

        while len(books) < num_books:
            
            url = f'{url}&page={page}'

            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, "html.parser")

                book_containers = soup.find_all("div", {"class": "s-result-item"})
                for book in book_containers:
                    title = book.find("h2", attrs={"class": "a-text-normal"})
                    author = book.find("span", {"class": "a-size-base"})
                    price = book.find("span", {"class": "a-price-whole"})
                    rating = book.find("span", {"class": "a-icon-alt"})

                    if title and author and price and rating:
                        book_title = title.text.strip()

                        if book_title not in seen_titles:
                            seen_titles.add(title.text)
                            books.append({
                                "Title": book_title,
                                "Author": author.find_next_sibling("span").text.strip(),
                                "Price": price.text.strip(),
                                "Rating": rating.text.strip()
                            })

                page += 1
            else:
                print("No books found")
                break

        # Limit the number of books to extract
        books = books[:num_books]

        # Convert this dictionary to a pandas DataFrame
        df = pd.DataFrame(books)

        # Drop duplicates
        df.drop_duplicates(subset="Title", inplace=True)

        return df

    @task()
    def transform_book_data(raw_books: pd.DataFrame):

        # Transform the data
        transformed_books = raw_books

        # check data validity
        if transformed_books.empty:
            raise ValueError('No books found')

        return transformed_books.to_dict(orient='records')

    @task()
    def load_book_data(transformed_books: dict):

        # check data validity
        if not transformed_books:
            raise ValueError('No books found')
        
        # Load the data via the hook
        postgres_hook = PostgresHook(postgres_conn_id='books_connection')
        insertion_query = """
        INSERT INTO books (title, author, price, rating)
        VALUES (%s, %s, %s, %s)
        """

        for book in transformed_books:
            postgres_hook.run(insertion_query, parameters=(book["Title"], book["Author"], book["Price"], book["Rating"]))

        pass

    create_sql_tables()
    raw_book_data = extract_book_data()
    transformed_book_data = transform_book_data(raw_book_data)
    load_book_data(transformed_book_data)

fetch_and_store_amazon_books_dag = fetch_and_store_amazon_books_etl()
