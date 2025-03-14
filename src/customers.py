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
    common.save_processed(df)
    print(df)
    return df


    print(df)
    return df

def load(df):
    print("Metodo LOAD dei clienti")
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = """
            CREATE TABLE customers (
            pk_customer VARCHAR PRIMARY KEY,
            region VARCHAR,
            city VARCHAR,
            cap VARCHAR
            );
            """

            try:
                cur.execute(sql)
            except psycopg.errors.DuplicateTable as ex:
                conn.commit()
                print(ex)
                domanda = input("Vuoi cancellare la tabella? si/no")
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
                       (pk_customer, region, city, cap)
                       VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;
                       """
            common.caricamento_barra(df, cur, sql)
            conn.commit()

def complete_city_region():
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:

        sql = """
        SELECT * FROM 
        """
        pass




def main ():
    print("Metodo MAIN dei clienti")
    df = extract()
    df = transform(df)
    load(df)

if __name__ == "__main__":
    main()