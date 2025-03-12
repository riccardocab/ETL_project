import os
import psycopg
import pandas as pd
from dotenv import load_dotenv

from src.common import read_file, caricamento_percentuale, caricamento_barra

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")

# 3 metodi per fare ETL

def extract():
    print("Metodo EXTRACT dei clienti")
    df = read_file()
    return df

def transform(df):
    print(df)
    print("Metodo TRANSFORM dei clienti")
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
            caricamento_barra(df, cur, sql)
            conn.commit()




def main ():
    print("Metodo MAIN dei clienti")
    df = extract()
    df = transform(df)
    load(df)

if __name__ == "__main__":
    main()