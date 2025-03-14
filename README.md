# TITOLO
## SOTTOTITOLO

**GRASSETTO**
_CORSIVO_

**customers**
- pk_customer VARCHAR
- region VARCHAR
- city VARCHAR
- cap VARCHAR

**categories**
- pk_category SERIAL
- name(ENG) VARCHAR

**products**
- pk_product VARCHAR
- fk_category INT
- name_length INT
- description_length INT
- imgs_qty INT
- 
**orders**
- pk_orders VARCHAR
- fk_customer VARCHAR
- status VARCHAR
- purchase_timestamp TIMESTAMP
- delivered_timestamp TIMESTAMP
- estimated_date DATE

**sellers**
- pk_seller VARCHAR
- region VARCHAR

**orders products**
- pk_order_product VARCHAR
- fk_order VARCHAR 
- fk_product VARCHAR
- fk_seller VARCHAR
- price FLOAT
- freight (costo trasporto) FLOAT

# TODO OPZIONALE
- copia del file in input alla cartella raw 
(fare in modo che il nome del file sia univoco, 
con data e ora)
- prima di fare il load creare database da Python
- controllo di validità per cancellare la tabella
(con user e psw)
- metodo per controllo di validità degli input,
oltre a strip() e upper()/lower()
- colonna che tracci la data di inserimento dei dati
- check sui cap >5 cifre (formato 01234)
- check sulle 20 regioni ammesse
- integrare dati customer a partire dal cap
- gestione del tipo di valore da aggiornare in fillNulls ()