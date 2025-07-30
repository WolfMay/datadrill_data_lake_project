
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


# Rešenje

#### 1. Bronze layer
Fajlovi se učitavaju u pandas data frame. Pošto smo svesni koji su tipovi podataka otprilike u pitanju, stavljen je constraint na podatke prilikom učitavanja da moraju da odgovaraju određenom tipu podatka. Takođe se nakon učitavanja tipovi još jednom validiraju pre nego što se čuvaju u parquet fajl.

#### 2. Silver layer
Start date se prebacuje iz stringa u date format, ukoliko je nevalidan non-null datum, skripta će to logovati. Računa se tenure_in_months za svakog zaposlenog i dodaje se kao nova kolona u employees tabelu. Salaries se enrichuje tako što se left joinuje prvo sa employees dataframeom a zatim se sve to left joinuje sa departments dataframeom. Ovde je možda mogla da se odradi neka veća normalizacija, da se departments razbije na departments i locations i da im se dodele primarni ključevi i da tako postoje u silver folderu ali sam odustao od toga jer je relativno jednostavna struktura. Takođe, moglo je ovde i da se priča o tome da li je manager employee i kako to da se hendla, ali pošto nije report zavisio od ovoga odlučio sam da preskočim u interesu vremena. Mogla je takođe da se napravi neka history dimenzija za salaries koja bi pratila promene plata kroz vreme.

#### 3. Gold layer
Probao sam da iskoristim salaries enriched tabelu za sve u ovom koraku.

Pravi se summary report csv fajl u sledećim koracima:
- Iz salaries enriched tabele se uzimaju svi unique zaposleni po employee id-u i grupiše se po lokaciji da bi se sabrao broj zaposlenih na svakoj lokaciji i rezultat se čuva u zaseban dataframe
- Filtrira se poslednja plata svakog zaposlenog jer je samo ona relevantna za report i čuva se u zaseban dataframe
- Zatim se ta tabela grupiše po departmanu i računaju se prosečna bruto plata i prosečan staž po departmanu i čuva se u zaseban dataframe
- Na taj dataframe se potom joinuje se enriched_salaries po locations koloni i iz nje se samo ova kolona selektuje i priključuje prethodnom dataframeu
- Naposletku, proverava se koji departman ima najduži prosečan staž i dodaje se nova kolona koja u sebi sadrži tu informaciju.

#### 4. Ostalo
##### 4.1 Vizualizacija u Matplotlibu
Kreirana su 3 bar charta koja prikazuju 3 zahteva iz izveštaja:
- prosečnu bruto platu po departmanu
- broj zaposlenih po lokaciji
- prosečni staž po departmanu

Chartovi su sačuvanu u .png formatu u visualization/ folderu.
##### 4.2 CLI skripta
Napravljena je cli skripta, očekuje input i output putanju kao parametre. Proverava da li fajlovi postoje pre nego što pređe na naredni korak, takođe pre početka proverava da li postoji svi output folderi i ako ne postoje, pravi ih.
##### 4.3 Uputstvo za upotrebu
###### 1. Klonirajte projekat
```
git clone https://github.com/WolfMay/datadrill_data_lake_project.git
```
###### 2. Pređite u datadrill_data_lake_project/scripts folder
```
cd .../datadrill_data_lake_project/scripts
```
###### 3. Opciono: Kreirajte virtuelno okruženje
Ovde bih preporučio da se koristi virtual environment koristeći ``` conda ``` ili ``` venv ```.
```
python -m venv venv
```
###### 3.1 Opciono: Aktivirajte virtuelno okruženje
Za Windows(Command Prompt)
```
venv\Scripts\activate.bat
```
Za Windows(PowerShell)
```
venv\Scripts\Activate.ps1
```
Za Linux/MacOS
```
source venv/bin/activate
```
###### 4. Instalirajte pakete iz requirements.txt fajla
```
pip install -r requirements.txt
```
###### 5. Pokrenite run_pipeline.py skriptu
```
python -m run_pipeline --input_folder .. --output_folder ..
```
Ovo pretpostavlja da se nalazite u scripts folderu i da niste dirali input .csv fajlove. Skripta će napraviti output foldere u root folderu projekta. Ako stavite neku drugu putanju za output, skripta će tamo napraviti sve potrebne foldere da bi pokrenula pipeline.
