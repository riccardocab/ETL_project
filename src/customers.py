import datetime
import os
import psycopg
import pandas as pd
from dotenv import load_dotenv

import src.common as common
load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

# 3 metodi per fare ETL

def extract():
    print("Metodo EXTRACT dei clienti")
    df = common.read_file()
    return df

def transform(df):
    print("Metodo TRANSFORM dei clienti")
    df = common.drop_duplicates(df)
    df = common.check_null(df,["customer_id"])
    df = common.format_string(df, ["region", "city"])
    df = common.format_cap(df)
    #common.save_processed(df)
    return df


    print(df)
    return df

def load(df):
    df["last_updated"] = datetime.datetime.now().isoformat(sep=" ", timespec="seconds")
    print("Metodo LOAD dei clienti")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            CREATE TABLE customers (
            pk_customer VARCHAR PRIMARY KEY,
            region VARCHAR,
            city VARCHAR,
            cap VARCHAR,
            last_updated TIMESTAMP
            );
            """

            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la tabella? si/no").lower().strip()
                if domanda == "si":
                #Se risponde si, cancellare tabella
                    sqldelete = """
                    DROP TABLE customers
                    """
                    cur.execute(sqldelete)
                    conn.commit()
                    print("Ricreo la tabella customers...")
                    cur.execute(sql)



            sql = """
            INSERT INTO customers
            (pk_customer, region, city, cap, last_updated)
            VALUES (%s, %s, %s, %s, %s) ON CONFLICT (pk_customer) DO UPDATE SET 
            (region, city, cap, last_updated) = (EXCLUDED.region, EXCLUDED.city, EXCLUDED.cap, EXCLUDED.last_updated);
            """
            common.caricamento_barra(df, cur, sql)
            conn.commit()

def complete_city_region():
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:

            sql = f"""
            UPDATE customers c1
            SET region = c2.region, last_updated = '{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")}'
            FROM customers c2
            WHERE c1.cap = c2.cap 
            AND c1.cap <> 'NaN' 
            AND c2.cap <> 'NaN'
            AND c1.region = 'NaN' 
            AND c2.region <> 'NaN'
            RETURNING *;
            """

            cur.execute(sql)

            print("Record con regione aggiornata")

            for record in cur:
                print(record)

            sql = f"""
                UPDATE customers c1
                SET city = c2.city, last_updated = 
                '{datetime.datetime.now().isoformat(sep=" ", timespec="seconds")}'
                FROM customers c2
                WHERE c1.cap = c2.cap 
                AND c1.cap <> 'NaN' 
                AND c2.cap <> 'NaN'
                AND c1.city = 'NaN' 
                AND c2.city <> 'NaN'
                RETURNING *;
                """

            cur.execute(sql)

            print("Record con city aggiornata")

            for record in cur:
                 print(record)

def main ():
    print("Metodo MAIN dei clienti")
    df = extract()
    df = transform(df)
    print("Dati trasformati")
    load(df)

if __name__ == "__main__":
    main()