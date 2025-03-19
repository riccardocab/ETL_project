import datetime
import os
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pandas import isnull
import psycopg
import dotenv

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def read_file():
    isValid = False
    df = pd.DataFrame
    while not isValid:
        path = input("Inserisci l'url del file: ").strip()
        try:
            path_list = path.split(".")

            if path_list[-1] == "csv" or path_list[-1] == "txt":
                df = pd.read_csv(path)
            elif path_list[-1] == "xlsx" or path_list[-1] == "xls":
                df = pd.read_excel(path)
            else:
                df = pd.read_json(path)
        except FileNotFoundError as ex:
            print(ex)
        except OSError as ex:
            print(ex)
        else:
            print("Path inserito correttamente")
            isValid = True
    else:
        return df

def caricamento_percentuale(df, cur, sql):
    # eseguo la query per caricare i dati (il risultato del caricamento è in percentuale)
    print(f"Caricamento in corso... {str(len(df))} righe da inserire.")
    perc_int = 0
    for index, row in df.iterrows():
        perc = float("%.2f" % ((index + 1) / len(df) * 100))
        if perc >= perc_int:
            print(f"{round(perc)}% Completato")
            perc_int += 5
        cur.execute(sql, row.to_list())

def caricamento_barra(df,cur,sql):
    print(f"Caricamento in corso... \n{str(len(df))} righe da inserire.")
    Tmax = 50
    if len(df)/2 < 50:
        Tmax = len(df)
    print("┌" + "─" * Tmax + "┐")
    print("│",end="")
    perc_int = 2
    for index, row in df.iterrows():
        perc = float("%.2f" % ((index + 1) / len(df) * 100))
        if perc >= perc_int:
            print("\r│" + "█" * (perc_int//2) + str(int(perc)) + "%",end="")
            #print(perc,end="")
            perc_int += 2
        cur.execute(sql, row.to_list())
    print("\r│" + "█" * Tmax + "│ 100% Completato!")
    print("└" + "─" * Tmax + "┘")

def format_cap(df):
    # Converte in stringa e riempie con zeri fino a 5 cifre
    #if "cap" in df.columns:
    #df["cap"].fillna(0).astype(int).astype(str).str.zfill(5)
    df["cap"] = df["cap"].apply(lambda cap: str(int(cap)).zfill(5) if cap == cap else cap)
    print("Ciao questo è il formar_cap")
    return df

def drop_duplicates(df):
    print(f"Valori duplicati rimossi: {df.duplicated().sum()}")
    df.drop_duplicates(inplace=True)
    return df

def check_null(df, subset=""):
    print(f"Valori nulli per colonna:\n {df.isnull().sum()} \n")
    subset = df.columns.tolist()[0] if not subset else subset
    df.dropna(subset=subset, inplace=True, ignore_index=True)
    #df = fill_null(df)
    #print(df)
    return df

def fill_null(df):
    #gestione del tipo di valore da aggiornare
    df.fillna(value = "nd", axis = 0, inplace = True)
    return df

def format_string(df, cols):
    #print(df[cols])
    for col in cols:
        df[col] = df[col].str.strip("./_ ")
        df[col] = df[col].str.replace("[0-9]", "", regex=True)
        df[col] = df[col].str.replace("[\\[\\]$&+:;=?@#|<>.^*(/_)%!]", "", regex=True)
        df[col] = df[col].str.replace(r"/s+", " ", regex=True)
    return df


def save_processed(df):
    name = input("Qual'è il nome del file? ").strip().lower()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"{name}_processed_datetime_{timestamp}.csv"
    #file_name = name + "processed" + "datetime" + str(datetime.datetime.now())
    print(file_name)
    if __name__== "__main__":
        directory_name = "../data/processed/"
    else:
        directory_name = "data/processed/"
    df.to_csv(directory_name + file_name, index=False)

def format_region():
    print("Formattazione dei nomi delle regioni per PowerBi")
    nome_tabella = input("Inserisci il nome della tabella da modificare: ").strip().lower()
    with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
        with conn.cursor() as cur:
            sql = f"""
            UPDATE {nome_tabella}
            SET region = 'Emilia-Romagna'
            WHERE region = 'Emilia Romagna'
            RETURNING *
            """
            cur.execute(sql)
            print("Record con regione aggiornata \n")
            for record in cur:
                print(record)

            sql = f"""
            UPDATE {nome_tabella}
            SET region = 'Friuli-Venezia Giulia'
            WHERE region = 'Friuli Venezia Giulia'
            RETURNING *
            """
            cur.execute(sql)
            print("Record con regione aggiornata \n")
            for record in cur:
                print(record)

            sql = f"""
            UPDATE {nome_tabella} 
            SET region = 'Trentino-Alto Adige'
            WHERE region = 'Trentino Alto Adige'
            RETURNING *
            """
            cur.execute(sql)
            print("Record con regione aggiornata \n")
            for record in cur:
                print(record)

if __name__ == "__main__":
    #df = read_file()
    #print("Visualizza dati prima di format_cap")
    #print(df)
    #df = format_cap(df)
    #print("Visualizza dati dopo di format_cap")
    #print(df)
    #check_null(df, ["customer_id"])
    #save_processed(df)
    #df = format_string(df, ["region", "city"])
    #print(df)
    format_region()



