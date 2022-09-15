
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


def _extract_articles():
    base_url = 'https://www.spiegel.de/international/'
    response = requests.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')

    pagination = soup.find(
        'div', class_='text-center text-black dark:text-shade-lightest font-normal font-sansUI lg:text-base md:text-base sm:text-s leading-normal')
    page_no = int(pagination.text.split()[-1])
    # Getting the current date and time
    dt = datetime.now(timezone.utc)
    # getting the timestamp
    ingestion_date = str(dt)
    update_time = str(dt)
    scraped_article = []
    try:

        for page in range(2, page_no):
            page_url = base_url+'p'+str(page)+'/'
            response = requests.get(page_url)
            soup1 = BeautifulSoup(response.text, 'html.parser')
            articles = soup1.find_all('article')
            for article in articles:
                data = {
                    'article_id': article['data-sara-article-id'],
                    'sub_title': article['aria-label'],
                    'title': article.header.h2.a.span.text,
                    'abstract': article.div.section.a.span.text,
                    'ingestion_time': ingestion_date,
                    'update_time': update_time
                }
                scraped_article.append(data)
                print(data)
    except Exception:
        pass
    df = pd.DataFrame.from_dict(scraped_article)
    df.to_csv('/tmp/articels.csv', index=False, header=True)


def _store_articles_to_database():
    db_hook = PostgresHook(
        postgres_conn_id='postgres_default', schema='airflow')
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

with DAG('news_articles_pipeline', default_args=default_args, schedule_interval='15 0 * * *', catchup=False) as dag:
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
    extract_articles = PythonOperator(
        task_id='extract_articles',
        python_callable=_extract_articles
    )

    store_articles = PythonOperator(
        task_id='store_articles',
        python_callable=_store_articles_to_database

    )
create_table >> extract_articles >> store_articles
