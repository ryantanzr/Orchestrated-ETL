

from bs4 import BeautifulSoup

# Dag defintion
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task, dag, task_group
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
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

    @task(task_id="create_sql_tables")
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
            price FLOAT,
            book_type VARCHAR(255),
            rating FLOAT,
            rating_count INT
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        cursor.close()
        connection.close()

    # Tasks: Extraction of amazon book data, Transformation of data, Loading of data
    @task(task_id="extract_book_data")
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
                    book_type = book.find("a", {"class": "a-size-base a-link-normal s-underline-text s-underline-link-text s-link-style a-text-bold"})
                    rating = book.find("span", {"class": "a-icon-alt"})
                    rating_count = book.find("span", {"class": "a-size-base s-underline-text"})

                    if title and author and price and book_type and rating and rating_count:
                        book_title = title.text.strip()

                        if book_title not in seen_titles:
                            seen_titles.add(title.text)
                            books.append({
                                "Title": book_title,
                                "Author": author.find_next_sibling("span").text.strip(),
                                "book_type": book_type.text.strip(),
                                "Price": price.text.strip(),
                                "Rating": rating.text.strip(),
                                "Rating_count": rating_count.text.strip()
                            })

                page += 1
            else:
                print("No books found")
                break

        # Limit the number of books to extract
        books = books[:num_books]

        # Convert this dictionary to a pandas DataFrame
        df = pd.DataFrame(books)

        return df
    
    @task_group(group_id="book_data_transformations")
    def book_data_transformations(books: pd.DataFrame):

        # Perform data cleaning and standardisation before transformations
        @task(task_id="standardise_book_data")
        def standardise_book_data(raw_books: pd.DataFrame):

            # check data validity
            if raw_books.empty:
                raise ValueError('No books found')

            standardise_books = raw_books

            # Drop duplicates
            raw_books.drop_duplicates(subset="Title", inplace=True)

            # Price must be a number
            standardise_books['Price'] = pd.to_numeric(standardise_books['Price'])

            # Rating must be a number
            standardise_books['Rating'] = standardise_books['Rating'].str.extract(r'(\d\.\d)')
            standardise_books['Rating'] = pd.to_numeric(standardise_books['Rating'])

            # Rating count must be a number
            standardise_books['Rating_count'] = standardise_books['Rating_count'].str.replace(',', '')
            standardise_books['Rating_count'] = pd.to_numeric(standardise_books['Rating_count'])


            return standardise_books

        # Perform data enrichment (Average rating by author, average price by author, rating distribution)
        @task(task_id="enrich_book_data")
        def enrich_book_data(standardised_books: pd.DataFrame):

            # Transform the data
            transformed_books = standardised_books

            # check data validity
            if transformed_books.empty:
                raise ValueError('No books found')

            return transformed_books.to_dict(orient='records')
        
        return enrich_book_data(standardise_book_data(books))

    @task(task_id="load_book_data")
    def load_book_data(transformed_books: dict):

        # check data validity
        if not transformed_books:
            raise ValueError('No books found')
        
        # Load the data via the hook
        postgres_hook = PostgresHook(postgres_conn_id='books_connection')
        insertion_query = """
        INSERT INTO books (title, author, price, book_type, rating, rating_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        for book in transformed_books:
            postgres_hook.run(insertion_query, parameters=(book["Title"], book["Author"], book["Price"], book["book_type"] ,book["Rating"], book["Rating_count"]))

        pass

    create_sql_tables()
    raw_book_data = extract_book_data()
    transformed_book_data = book_data_transformations(raw_book_data)
    load_book_data(transformed_book_data)

fetch_and_store_amazon_books_dag = fetch_and_store_amazon_books_etl()