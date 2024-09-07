from dagster import job, op
import pandas as pd
import psycopg2
import requests

@op
def fetch_data():
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
    data = pd.read_csv(url)
    return data

@op
def clean_data(df: pd.DataFrame):
    # Simplify data structure and prepare for PostgreSQL
    df = df.melt(id_vars=["Province/State", "Country/Region", "Lat", "Long"], var_name="Date", value_name="Cases")
    df['Date'] = pd.to_datetime(df['Date'])
    return df.drop_duplicates()

@op
def store_data(df: pd.DataFrame):
    connection = psycopg2.connect(host='postgres', dbname='covid_data', user='user', password='password')
    cursor = connection.cursor()
    # Assuming table 'covid_cases' has been created previously
    psycopg2.extras.execute_batch(cursor, """
    INSERT INTO covid_cases (province_state, country_region, lat, long, date, cases) 
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING
    """, df.values.tolist())
    connection.commit()
    cursor.close()
    connection.close()

@job
def covid_data_pipeline():
    raw_data = fetch_data()
    cleaned_data = clean_data(raw_data)
    store_data(cleaned_data)
