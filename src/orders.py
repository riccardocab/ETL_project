# ETL methods for ORDERS

import pandas as pd
import src.common as common
#from src.common import read_file, caricamento_barra
import psycopg
from dotenv import load_dotenv
import os
import datetime

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

def extract():
    print("---EXTRACT ORDERS--- ")
    df = common.readfile()
    return df

def transform(df):
    print("---TRANSFORM ORDERS---")
    df = common.drop_duplicates(df)
    df = common.check_null(df, ["order_id", "customer_id"])
    return df


def load(df):
    print("---LOAD ORDERS---")
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    df["order_purchase_timestamp"] = pd.to_datetime(df["order_purchase_timestamp"])
    df["order_delivered_customer_date"] = pd.to_datetime(df["order_delivered_customer_date"])
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:

            sql = """
                CREATE TABLE orders (
                pk_order VARCHAR PRIMARY KEY,
                fk_customer VARCHAR,
                status VARCHAR,
                purchase_timestamp TIMESTAMP,
                delivered_timestamp TIMESTAMP,
                estimated_date DATE,
                last_updated TIMESTAMP,
                FOREIGN KEY(fk_customer) REFERENCES customers (pk_customer)
                );
                """
            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                print(ex)
                conn.commit()
                delete = input("Do you want to delete table? Y/N ").upper().strip()
                if delete == "Y":
                    sql_delete = """ 
                    DROP TABLE orders CASCADE;
                    """
                    cur.execute(sql_delete)
                    conn.commit()
                    print("Recreating orders table")
                    cur.execute(sql)


            sql = """
            INSERT INTO orders
            (pk_order, fk_customer, status, purchase_timestamp, delivered_timestamp, estimated_date, last_updated)
            VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (pk_order) DO UPDATE SET 
            (fk_customer, status, purchase_timestamp, delivered_timestamp, estimated_date, last_updated) = (EXCLUDED.fk_customer, EXCLUDED.status, EXCLUDED.purchase_timestamp, EXCLUDED.delivered_timestamp, EXCLUDED.estimated_date, EXCLUDED.last_updated)
            """

            common.loading_bar(df, cur, sql)
            conn.commit()

            sql = """
            UPDATE orders SET delivered_timestamp = null
            WHERE EXTRACT (YEAR FROM delivered_timestamp) = 48113
            """

            cur.execute(sql)
            conn.commit()



def main():
    df = extract()
    df = transform(df)
    load(df)


if __name__ == "__main__":
    main()