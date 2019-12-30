import sqlite3
from datetime import datetime

import pandas as pd

# Create database
con = sqlite3.connect("communes.db")
print(datetime.today(), "communes.db has been created")

# Cursor for excuting SQLs
cur = con.cursor()

# Create Balances table
create_balances_tbl_sql = """
CREATE TABLE BALANCES(
            ID INTEGER PRIMARY KEY AUTOINCREMENT,
            EXER VARCHAR(4),
            IDENT VARCHAR(14),
            NDEPT VARCHAR(3),
            LBUDG VARCHAR(40),
            INSEE VARCHAR(3),
            CBUDG VARCHAR(1),
            CTYPE VARCHAR(3),
            CSTYP VARCHAR(2),
            NOMEN VARCHAR(9),
            SIREN VARCHAR(9),
            CREGI VARCHAR(3),
            CACTI VARCHAR(2),
            SECTEUR VARCHAR(1),
            FINESS VARCHAR(9),
            CODBUD1 VARCHAR(1),
            CATEG VARCHAR(7),
            BAL VARCHAR(4),
            COMPTE VARCHAR(8),
            BEDEB DECIMAL,
            BECRE DECIMAL,
            OBNETDEB DECIMAL,
            OBNETCRE DECIMAL,
            ONBDEB DECIMAL,
            ONBCRE DECIMAL,
            OOBDEB DECIMAL,
            OOBCRE DECIMAL,
            SD DECIMAL,
            SC DECIMAL)"""

cur.execute(create_balances_tbl_sql)
print(datetime.today(), "BALANCES table created")

# Create Comptes table
create_comptes_tbl_sql = """
CREATE TABLE COMPTES(
            COMPTE INTEGER PRIMARY KEY,
            LIBELLE VARCHAR(200))"""

cur.execute(create_comptes_tbl_sql)
print(datetime.today(), "COMPTES table created")

# Load comptes data into the date
## Read ODS file
comptes_data = pd.read_excel("M14_nomenclatures.ods", engine="odf", sheet_name="M14 2018", skiprows=2).drop_duplicates()

## Insert rows
insert_comptes_sql = """INSERT INTO COMPTES (COMPTE,LIBELLE) VALUES (?,?)"""

for row_num, row_data in comptes_data.iterrows():
    try:
        cur.execute(insert_comptes_sql, (row_data["Compte"], row_data["Libellé"]))
    except:
        print(row_data["Compte"])
con.commit()

con.close()
