# unique method for extracting files
import os

import pandas as pd
import datetime
import psycopg
from dotenv import load_dotenv

load_dotenv()
host = os.getenv("host")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
port = os.getenv("port")


def readfile():
    path = input("Insert file path: ")
    is_valid = False
    df = pd.DataFrame()
    pd.set_option("display.max.rows", None)
    pd.set_option("display.max.columns", None)
    pd.set_option("display.width", None)
    while not is_valid:
        try:
            path_list = path.split(".")

            if path_list[-1] == "csv" or path_list[-1] == "txt":
                df = pd.read_csv(path)
            elif path_list[-1] == "xlsx" or path_list[-1] == "xls":
                df = pd.read_excel(path)
            else:
                df = pd.read_json(path)

            # inserire percorso assoluto
        except FileNotFoundError as ex:
            print(ex)
        except OSError as ex:
            print(ex)
        else:
            print("Path entered correctly")
            is_valid = True
    else:
        return df


def loading_bar(df, cur, sql):
    print(f"Loading... \n{str(len(df))} rows to insert.")
    tmax = 50
    if len(df) / 2 < 50:
        tmax = len(df)
    print("┌" + "─" * tmax + "┐")
    print("│", end="")
    perc_int = 2
    for index, row in df.iterrows():
        perc = float("%.2f" % ((index + 1) / len(df) * 100))
        if perc >= perc_int:
            print("\r│" + "█" * (perc_int // 2) + str(int(perc)) + "%", end="")
            # print(perc,end="")
            perc_int += 2
        cur.execute(sql, row.to_list())
    print("\r│" + "█" * tmax + "│ 100% Completed!")
    print("└" + "─" * tmax + "┘")


def format_cap(df):
    # df["cap"] = df["cap"].fillna(0).astype(int).astype(str).str.zfill(5)
    df["cap"] = df["cap"].apply(lambda cap: str(int(cap)).zfill(5) if cap == cap else cap)
    return df


def format_region():
    print("Formatting region name for PowerBi")
    table_name = input("Enter the name table to be modified: ").strip().lower()
    try:
        with psycopg.connect(host=host, dbname=dbname, user=user, password=password, port=port) as conn:
            with conn.cursor() as cur:
                updates = [
                    ("Emilia-Romagna", "Emilia Romagna"),
                    ("Friuli-Venezia Giulia", "Friuli Venezia Giulia"),
                    ("Trentino-Alto Adige", "Trentino AltoAdige")
                ]

                for new_region, old_region in updates:
                    sql = f"""
                    UPDATE {table_name}
                    SET region = %s
                    WHERE region = %s
                    RETURNING *;
                    """
                    cur.execute(sql, (new_region, old_region))
                    for record in cur:
                        print(record)

                print("All updates applied successfully.\n")

    except psycopg.errors.UndefinedTable:
        print(f"Error: The table '{table_name}' does not exist in the database.")
    except Exception as ex:
        print(f"Database error:{ex}")


def drop_duplicates(df):
    print(f"There are {df.duplicated().sum()} duplicates")
    dup = input("Do you want to delete the duplicates? Y/N ").upper().strip()
    if dup == "Y":
        df.drop_duplicates(inplace=True)
    return df


def check_null(df, subset=""):
    print(f"Null values per column:\n {df.isnull().sum()} \n")
    subset = df.columns.tolist()[0] if not subset else subset
    df.dropna(subset=subset, inplace=True, ignore_index=True)
    return df


def fill_null(df):
    # todo gestione tipo di valore da aggiornare
    df.fillna(value="nd", axis=0, inplace=True)
    return df

# Salvataggio


def save_processed(df):
    name = input("What is the name of the file? ").strip().lower()
    timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S")
    file_name = name + "_processedDateTime_" + timestamp + ".csv"
    print(file_name, end="\n\n")
    if __name__ == "__main__":
        directory_name = "../data/processed/"
    else:
        directory_name = "data/processed/"
    df.to_csv(directory_name + file_name, index=False)


def format_string(df, cols):
    for col in cols:
        df[col] = df[col].str.strip()
        df[col] = df[col].str.replace("[0-9]", "", regex=True)
        # se avessi scritto [^0-9] avrebbe lasciato SOLO i caratteri numerici
        df[col] = df[col].str.replace("[\\[\\]$&+:;=?@#|<>.^*(/_)%!]", "", regex=True)
        df[col] = df[col].str.replace(r"\s+", " ", regex=True)
    return df


if __name__ == "__main__":
    #df_test = readfile()
    #print("----RAW DATA----")
    #print(df_test["cap"])
    # df = format_string(df_test, ["region", "city"])
    #df_test = format_cap(df_test)
    #print("----MODIFICA CAP----")
    #print(df_test["cap"])
    # check_null(df,[])
    # save_processed(df)
    format_region()
