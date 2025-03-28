## Project work Olist - Data Engineer


## Descrizione
In questo progetto sono stati sviluppati gli ETL (Extract, Transform, Load) per la gestione e l'elaborazione dei dati relativi a clienti, categorie, prodotti, ordini e venditori. Include anche un'interfaccia grafica per facilitare l'esecuzione delle operazioni.

## Funzionalità
- **ETL per diversi dataset**: clienti, categorie, prodotti, ordini, venditori e relazioni tra ordini e prodotti.
- **Formattazione e integrazione dati**: gestione delle regioni e associazione città-regione.
- **Anteprima con Jupyter Notebook**: permette di aprire file `.ipynb` direttamente dall'interfaccia.
- **Integrazione con Power BI**: facilita l'apertura di progetti Power BI `.pbix` per l'analisi dei dati.

## Requisiti
- **Python 3.x**
- Librerie necessarie installabili con:
  ```
  pip install -r requirements.txt
  ```
- **Jupyter Notebook** (se si desidera aprire file `.ipynb`)
- **Power BI Desktop** (se si intende aprire file `.pbix` altrimenti visionare il pdf)

## Installazione
1. Clonare il repository o scaricare i file.
   ```bash
   git clone <repository-url>
   ```
2. Installare moduli:
   ```bash
   pip install -r requirements.txt
   ```
3. Avviare l'applicazione:
   ```bash
   python main.py
   ```

## Utilizzo
1. Avviare `main.py` per aprire l'interfaccia grafica.
2. Selezionare le operazioni ETL disponibili.
3. Per l'analisi dati, utilizzare i pulsanti per aprire Jupyter Notebook o Power BI.

## Struttura del progetto
```
project-work/
│── main.py                 # Interfaccia grafica e gestione operazioni ETL
│── src/
│   ├── common.py           # Funzioni comuni
│   ├── etl/
│   │   ├── customers.py    # ETL per clienti
│   │   ├── categories.py   # ETL per categorie
│   │   ├── products.py     # ETL per prodotti
│   │   ├── orders.py       # ETL per ordini
│   │   ├── sellers.py      # ETL per venditori
│   │   ├── orders_products.py  # ETL per relazioni ordini-prodotti
```

## Autore
- Riccardo Cabriolu