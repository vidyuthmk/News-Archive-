
from cgitb import Hook
from fileinput import filename
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime, timedelta, timezone
from bs4 import BeautifulSoup
from airflow import DAG
import requests

# python operator function to web scrape all the articles from the web link


def _extract_articles():
    base_url = 'https://www.spiegel.de/international/'
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    # find the number of pages that needs to be traversed
    pagination = soup.find(
        'div', class_='text-center text-black dark:text-shade-lightest font-normal font-sansUI lg:text-base md:text-base sm:text-s leading-normal')
    page_no = int(pagination.text.split()[-1])
    # Getting the current date and time
    dt = datetime.now(timezone.utc)
    # getting the timestamp
    ingestion_date = str(dt)
    update_time = str(dt)
    # store all the records in an array to make save in organised way.
    scraped_article = []
    try:
        # loop through each page of internalion news.
        for page in range(2, page_no):
            page_url = base_url+'p'+str(page)+'/'
            response = requests.get(page_url)
            soup1 = BeautifulSoup(response.text, 'html.parser')
            articles = soup1.find_all('article')
            # find all the articles in the current page
            for article in articles:
                # save the record in dictionary to create a organised data.
                data = {
                    'article_id': article['data-sara-article-id'],
                    'sub_title': article['aria-label'],
                    'title': article.header.h2.a.span.text,
                    'abstract': article.div.section.a.span.text,
                    'ingestion_time': ingestion_date,
                    'update_time': update_time
                }
                # append the data into the array
                scraped_article.append(data)
                # printing to track the data just for reference on the task instance run
                print(data)
    except Exception:
        pass

    # creating a dataframe from the dictionay to save it as csv
    df = pd.DataFrame.from_dict(scraped_article)
    df.to_csv('/tmp/articels.csv', index=False, header=True)

# task funtion to store the extracted articles to postgres database


def _store_articles_to_database():
    # creating hook to perform the sql query
    db_hook = PostgresHook(
        postgres_conn_id='postgres_default', schema='airflow')
    # creating a dummy table so that to check duplicates before storing it to the database to avoide duplicates
    # conflict created on the unique Id of article_id
    db_hook.copy_expert(sql="""
                                CREATE TABLE temp_h (
                                ARTICLE_ID TEXT NOT NULL,
                                TITLE TEXT NOT NULL,
                                SUB_TITLE TEXT NOT NULL,
                                ABSTRACT TEXT NOT NULL,
                                INGESTION_TIME TEXT NOT NULL,
                                UPDATE_TIME TEXT NOT NULL
                                );
                                COPY temp_h FROM STDIN (FORMAT CSV);
                                INSERT INTO NEWS_ARTICLES(ARTICLE_ID, TITLE, SUB_TITLE,ABSTRACT,INGESTION_TIME,UPDATE_TIME)
                                SELECT *
                                FROM temp_h ON conflict (ARTICLE_ID) 
                                DO update set UPDATE_TIME=to_char(current_timestamp, 'HH12:MI:SS');
                                DROP TABLE temp_h;
                        """,
                        filename='/tmp/articels.csv')


# default arguments to schedule the job to run every 15 mins
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2019, 9, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
# scheduling the job for every 15mins
with DAG('news_articles_pipeline', default_args=default_args, schedule_interval='*/15 * * * *', catchup=False) as dag:

    # Task to perform table creation to save the scrapped articles
    create_table = PostgresOperator(
        task_id='create_table',
        sql='''
        CREATE TABLE IF NOT EXISTS NEWS_ARTICLES(
            ARTICLE_ID TEXT NOT NULL,
            TITLE TEXT NOT NULL,
            SUB_TITLE TEXT NOT NULL,
            ABSTRACT TEXT NOT NULL,
            INGESTION_TIME TEXT NOT NULL,
            UPDATE_TIME TEXT NOT NULL,
            UNIQUE(ARTICLE_ID)
        )
        '''
    )
    # Task to call the python function to scrape the data from the website
    extract_articles = PythonOperator(
        task_id='extract_articles',
        python_callable=_extract_articles
    )
    # Task to call the python funtion to save the data to the database.
    store_articles = PythonOperator(
        task_id='store_articles',
        python_callable=_store_articles_to_database

    )

# Task lineage (dependencies)
create_table >> extract_articles >> store_articles
