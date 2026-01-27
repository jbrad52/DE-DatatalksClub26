import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm
import click

def ingest_data(
        url: str,
        engine,
        target_table: str,
        #chunksize: int = 100000,
) -> pd.DataFrame:
    df = pd.read_parquet(
        url,
        #dtype=dtype,
        #parse_dates=parse_dates,
    )

    df.head(0).to_sql(
        name=target_table,
        con=engine,
        if_exists="replace"
    )

    print(f"Table {target_table} created")

    df.to_sql(
        name=target_table,
        con=engine,
        if_exists="append"
    )

    print(f'done ingesting to {target_table}')

@click.command()
@click.option("--pg-user", default="root", help="Postgres user")
@click.option("--pg-pass", default="root", help="Postgres password")
@click.option("--pg-host", default="localhost", help="Postgres host")
@click.option("--pg-port", default="5432", help="Postgres port")
@click.option("--pg-db", default="ny_taxi", help="Postgres database")
@click.option("--conn", default=None, help="Full SQLAlchemy connection string (overrides pg-*)")
@click.option("--year", default=2025, type=int, help="Year of the dataset")
@click.option("--month", default=11, type=int, help="Month of the dataset (1-12)")
#@click.option("--chunksize", default=100000, type=int, help="CSV chunk size")
@click.option("--target-table", default="green_taxi_data", help="Target DB table name")
#@click.option("--url-prefix", default="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow", help="URL prefix for the dataset")
def cli(pg_user, pg_pass, pg_host, pg_port, pg_db, conn, year, month, target_table):
    """
    Ingest NYC yellow taxi data into Postgres.
    """
    if conn:
        engine = create_engine(conn)
    else:
        conn_str = f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
        engine = create_engine(conn_str)

    url = f'green_tripdata_{year:04d}-{month:02d}.parquet'

    ingest_data(
        url=url,
        engine=engine,
        target_table=target_table,
        #chunksize=chunksize
    )

if __name__ == '__main__':
    cli()