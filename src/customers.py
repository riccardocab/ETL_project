# ETL method used for customers
import datetime

import src.common as common
import os
from dotenv import load_dotenv
import psycopg


load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def extract():
    print("---EXTRACT CUSTOMERS---")
    df = common.readfile()
    return df


def transform(df):
    print("---TRANSFORM CUSTOMERS---")
    df = common.drop_duplicates(df)
    df = common.check_null(df, ["customer_id"])
    df = common.format_string(df, ["region", "city"])
    df = common.format_cap(df)
    common.save_processed(df)
    return df


def load(df):
    print("---LOAD CUSTOMERS---")
    df["last_update"] = datetime.datetime.now().strftime("%Y_%m_%d_%H:%M:%S")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            CREATE TABLE customers (
            pk_customer character varying PRIMARY KEY,
            region character varying,
            city character varying,
            cap character varying,
            last_update TIMESTAMP
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
                    DROP TABLE customers;
                    """
                    cur.execute(sql_delete)
                    conn.commit()
                    print("Recreating customer table")
                    cur.execute(sql)
            sql = """
            INSERT INTO customers
            (pk_customer, region, city, cap,last_update)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (pk_customer) DO UPDATE 
            SET (region, city, cap, last_update) = (EXCLUDED.region, EXCLUDED.city, EXCLUDED.cap,EXCLUDED.last_update);
            """

            common.loading_bar(df, cur, sql)
            conn.commit()


def integrate_city_region():
    print("---INTEGRATE CITY & REGION---")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            SELECT *
            FROM customers 
            WHERE region = 'NaN' OR city = 'NaN';
            """
            cur.execute(sql)
            #print(f"List of NaN records: {cur.rowcount},they are: ")
            #for record in cur:
             #   print(record)

            sql = f"""
              UPDATE customers AS c1 
              SET region = c2.region, 
              last_update = '{datetime.datetime.now().strftime("%Y_%m_%d %H:%M:%S")}'
              FROM customers AS c2
              WHERE c1.cap = c2.cap
                    AND c1.cap <> 'NaN'AND c2.cap <> 'NaN'
                    AND c1.region = 'NaN' AND c2.region <> 'NaN' 
              RETURNING c1.*;
             """

            cur.execute(sql)
            updated_records = cur.fetchall()
            #for record in updated_records:
             #   print(record)
            sql = f"""
            UPDATE customers AS c1 
            SET city = c2.city,
            last_update ='{datetime.datetime.now().strftime("%Y_%m_%d %H:%M:%S")}'
            FROM customers AS c2
            WHERE c1.cap = c2.cap
                AND c1.cap <> 'NaN' AND c2.cap <> 'NaN'
                AND c1.city = 'NaN' AND c2.city <> 'NaN'
            RETURNING c1.*;
            """

            cur.execute(sql)

            #print(f"List of updated records:{cur.rowcount}, they are :")
            #for record in cur:
             #   print(record)

            print("Updated successfully!")
            conn.commit()


def main():
    df = extract()
    df = transform(df)
    load(df)


if __name__ == "__main__":
    main()