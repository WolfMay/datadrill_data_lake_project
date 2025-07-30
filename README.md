
#  Mini Data Lake – Zadatak za Python kandidata

## Zadatak

Zamislite da radite na implementaciji internog mini data lake-a. Klijent vam je poslao sledeće CSV fajlove:

- `employees.csv`
- `salaries.csv`
- `departments.csv`

### Vaš zadatak:

#### 1. Bronze layer
- Učitati CSV fajlove
- Validirati podatke
- Sačuvati ih u `bronze/` folder u Parquet formatu

#### 2. Silver layer
- Formatirati `start_date`
- Izračunati `tenure_in_months` po zaposlenom
- Obogatiti `salaries` podacima iz drugih fajlova
- Sačuvati kao Parquet fajlove u `silver/`

#### 3. Gold layer (izveštaj)
Napraviti CSV izveštaj:
- prosečna bruto plata po departmanu
- broj zaposlenih po lokaciji
- departman sa najdužim prosečnim stažom

Sačuvati kao `gold/summary_report.csv`.

#### 4. Ostalo
- Vizualizacija u Matplotlib/Seaborn
- CLI skripta `run_pipeline.py`
- README sa uputstvima
- requirements.txt

#### 5. Predaja
Pošaljite:
- GitHub link do koda
- README.md sa opisom rešenja

Srećno!
