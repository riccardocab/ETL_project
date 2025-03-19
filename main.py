import src.customers as customers
import  src.categories as categories
import src.products as products
from src import common

if __name__ == "__main__":
    risposta = "-1"
    while risposta != "0":
        risposta = input("""Che cosa vuoi fare?"
    1) ETL dei customers"
    2) Esegui integrazione dati e città
    3) Formatta nomi regione per PowerBI
    4) ETL di categories
    0) ESCI
    """)
        if risposta == "1":
            df_customers = customers.extract()
            df_customers = customers.transform(df_customers)
            customers.load(df_customers)
        elif risposta == "2":
            customers.complete_city_region()
        elif risposta == "3":
            common.format_region()
        elif risposta == "4":
            df_categories = categories.extract()
            df_categories = categories.macro_category(df_categories)
            categories.load(df_categories)
        else:
            risposta = "0"




    #products.extract()
    #products.transform()
    #products.load()