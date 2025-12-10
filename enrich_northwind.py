import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sqlalchemy.orm import declarative_base, sessionmaker
import pandas as pd
from dotenv import load_dotenv
from rich import print
import os
import click
from helper_functions import dataframe_to_sqla_class

## Load environment variables from .env file
load_dotenv()
db_host = os.getenv('DB_HOST', 'localhost')
db_port = os.getenv('DB_PORT', 5433)
db_name = os.getenv('DB_NAME', 'training')
db_user = os.getenv('DB_USER', 'postgres')
db_password = os.getenv('DB_PASSWORD', 'admin')

@click.command()
@click.option('--table_name', default='northwind_validated', help='Table name to read validated data from.')
@click.option('--schema_name', default='public', help='Schema name in the database.')
@click.option('--quotes', default='quotes_table', help='Name of the quotes table.')
@click.option('--weather', default='weather_data', help='Name of the weather table.')
def enrich_northwind(table_name, schema_name, quotes, weather):
    ## Read the validated northwind table from the database
    db_url = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(db_url)
    with engine.connect() as connection:
        query = f"SELECT * FROM {schema_name}.{table_name};"
        df_northwind = pd.read_sql(query, connection) # pd.dataframe
    print(f":white_check_mark: [bold]Successfully read {len(df_northwind)} rows from the database table '{table_name}'.[/bold]")

    ## Read the quotes table from the database
    with engine.connect() as connection:
        query = f"SELECT * FROM {schema_name}.{quotes};"
        df_quotes = pd.read_sql(query, connection) # pd.dataframe
    print(f":white_check_mark: [bold]Successfully read {len(df_quotes)} rows from the database table '{quotes}'.[/bold]")

    ## Read the weather data table from the database
    with engine.connect() as connection:
        query = f"SELECT * FROM {schema_name}.{weather};"
        df_weather = pd.read_sql(query, connection) # pd.dataframe
    print(f":white_check_mark: [bold]Successfully read {len(df_weather)} rows from the database table '{weather}'.[/bold]")

    ## Join the northwind data with the weather data, based on the northwind_validated.orderdate = weather_data.time
    df_northwind['orderdate'] = pd.to_datetime(df_northwind['orderdate'], errors='coerce').dt.date
    df_weather['time'] = pd.to_datetime(df_weather['time'], errors='coerce').dt.date
    df_enriched = pd.merge(df_northwind, df_weather[['time', 'temperature_2m_mean']], left_on='orderdate', right_on='time', how='left')
    df_enriched.drop(columns=['time'], inplace=True)
    print(f":white_check_mark: [bold]Successfully enriched northwind data with weather data.[/bold]")

    ## Add a random_quote column to the northwind data
    np.random.seed(42)  # for reproducibility
    random_quotes = np.random.choice(df_quotes['quote'], size=len(df_enriched), replace=True)
    df_enriched['random_quote'] = random_quotes
    print(f":white_check_mark: [bold]Successfully added random quotes to northwind data.[/bold]")

    ## Write the enriched data back to a new table in the database, if it doesn't exist yet !
    print(f":truck: [bold]Writing enriched data to the database table 'northwind_enriched'...[/bold]")
    tables_in_db = []
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}';"))
        tables_in_db = [row[0] for row in result.fetchall()]
    if 'northwind_enriched' in tables_in_db:
        print(f":warning: [bold]Table 'northwind_enriched' already exists in the database. Overwriting it...[/bold]")
        with engine.begin() as connection:
            connection.execute(text(f"DROP TABLE {schema_name}.northwind_enriched CASCADE;"))
    Base = declarative_base()
    EnrichedClass = dataframe_to_sqla_class(df_enriched, 'northwind_enriched', Base, schema_name=schema_name)
    Base.metadata.create_all(engine)  # Create the table if it doesn't exist
    Session = sessionmaker(bind=engine)
    session = Session()
    for _, row in df_enriched.iterrows():
        enriched_instance = EnrichedClass(**row.to_dict())
        session.add(enriched_instance)
    session.commit()
    session.close()
    print(f":white_check_mark: [bold]Successfully wrote enriched data to the database table 'northwind_enriched'.[/bold]")

    ## Create reporting tables
    create_customers_view = text(f"CREATE OR REPLACE VIEW {schema_name}.v_customers AS SELECT customerid, companyname, contactname, contacttitle, random_quote FROM {schema_name}.northwind_enriched;")
    create_orders_view = text(f"CREATE OR REPLACE VIEW {schema_name}.v_orders AS SELECT orderid, customerid, orderdate, requireddate, shippeddate, quantity, temperature_2m_mean FROM {schema_name}.northwind_enriched;")
    with engine.begin() as connection:
        connection.execute(create_customers_view)
        connection.execute(create_orders_view)
    # Check they are there
    with engine.connect() as connection:
        result = connection.execute(text(f"SELECT table_name FROM information_schema.views WHERE table_schema = '{schema_name}';"))
        views = [row[0] for row in result.fetchall()]
    if 'v_customers' in views and 'v_orders' in views:
        print(f":white_check_mark: [bold]Successfully created views 'v_customers' and 'v_orders' in the database.[/bold]")

if __name__ == '__main__':
    enrich_northwind()