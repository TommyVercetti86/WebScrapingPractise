import requests # - Hacer solicitudes HTTP y descargar el contenido
from bs4 import BeautifulSoup # - Analizar el contenido HTML
import csv
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import snowflake.connector
import os
from snowflake.connector.pandas_tools import write_pandas

# -------------------------------------- DATA EXTRACTION --------------------------------------

# -- variable con el URL de la página de Wikipedia
url = 'https://en.wikipedia.org/wiki/World_population'

# -- Descargamos el contenido de la página
response = requests.get(url) # - Se hace una solicitud con GET

# - El contenido de la respuesta se pasa a BeautifulSoup para analizar el HTML
soup = BeautifulSoup(response.content, 'html.parser') 

# -- Buscamos la tabla que deseamos extraer
table = soup.find('table', {'class': 'wikitable sortable'}) # - El método find() se utiliza para buscar un elemento HTML específico

# -- Creamos el DataFrame
data = [] # - Creamos una lista con los valores de la tabla
for row in table.find_all('tr'):
    cells = row.find_all('td')
    data.append([cell.text.strip() for cell in cells])

# -- Creamos el DataFrame usando los valores de data[] y unos identificadores para las columnas
df = pd.DataFrame(data, columns=['Region', 'Density', 'Population', 'Most Pop Country', 'Most Pop City'])

# -------------------------------------- DATA TRANSFORM --------------------------------------

# - Eliminamos las filas con valores nulos
df = df.dropna()

# - Eliminamos las filas con valores vacíos
df = df[df['Region'].str.strip() != '']

# - Convertirmos el tipo de las columnas numéricas a números
"""
df['Density'] = df['Density'].str.replace(',', '').astype(float)
df['Population'] = df['Population'].str.replace(',', '').astype(int)
"""
# -- Aparece error porque ~0 no puede convertirse a float
# - Para solucionarlo:
# --> replace('~0', '0'), errors='coerce' indica que se deben reemplazar los valores que no se pueden convertir


df['Density'] = pd.to_numeric(df['Density'].str.replace(',', '').replace('~0', '0'), errors='coerce')
df['Population'] = pd.to_numeric(df['Population'].str.replace(',', '').replace('None', '0'), errors='coerce')


# - Reemplazamos los valores inconsistentes
df['Density'].replace('None', 0, inplace=True)
df['Population'].replace('None', 0, inplace=True)

# - Reemplazamos el valor Nan del la fila Antartica y la columna Population por un 0 para permitir operaciones
df['Population'].fillna(0, inplace=True)

# - Guardamos el DataFrame procesado
df.to_csv('tabla_wikipedia_procesada.csv', index=False)

_ = os.system('cls')

print()
print("=================================")
print("Tabla guardada en archivo .CSV")
print("=================================")
print()


# -------------------------------------- DATA LOADING --------------------------------------

# -- Configuramos la conexión a la base de datos en Snowflake

print()
print("=================================")
print("Conectando a la base de datos...: ")
print("=================================")

try:
    
    # - Creamos los parámetros de la conexión
    account = 'fuivbuu-zk33410'
    user = 'ALEJANDRO'   
    password = '49236073L94tr1c14'
    #password = os.environ.get('SNOWFLAKE_PASSWORD')
    warehouse = 'PIPELINEWAREHOUSE'
    database = 'PIPLINER'
    schema = 'pipelineschema'
    role = 'ACCOUNTADMIN'
    
    # - Conectamos con Snowflake
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    
    # Crear un objeto del tipo cursor
    cur = conn.cursor()
    
    # Crear la tabla WorldPopulation si no existe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS WorldPopulation (
            ID INT AUTOINCREMENT PRIMARY KEY,
            Region VARCHAR,
            Density FLOAT,
            Population FLOAT,
            MostPopCountry VARCHAR,
            MostPopCity VARCHAR
        )
    """)
    # Inserts para agregar los datos
    insert_query = """
        INSERT INTO WorldPopulation (Region, Density, Population, MostPopCountry, MostPopCity)
        VALUES (%s, %s, %s, %s, %s)
    """
    
    data = [
        ('Asia', 104.1, 4641.0, '1,439,090,595 – India', '13,515,000 – Tokyo Metropolis(37,400,000 – G...)'),
        ('Africa', 44.4, 1340.0, '0,211,401,000 – Nigeria', '09,500,000 – Cairo(20,076,000 – Greater Cairo)'),
        ('Europe', 73.4, 747.0, '0,146,171,000 – Russia, approx. 110 million i...', '13,200,000 – Moscow(20,004,000 – Moscow metr...)'),
        ('Latin America', 24.1, 653.0, '0,214,103,000 – Brazil', '12,252,000 – São Paulo City(21,650,000 – São...)'),
        ('Northern America', 14.9, 368.0, '0,332,909,000 – United States', '08,804,000 – New York City(23,582,649 – New ...)'),
        ('Oceania', 5.0, 42.0, '0,025,917,000 – Australia', '05,367,000 – Sydney'),
        ('Antarctica', 0.0, 0.0, 'N/A', '00,001,258 – McMurdo Station')
    ]
    
    # Ejecutar los inserts
    cur.executemany(insert_query, data)
    print()

    print()
    print("=================================")
    print("Conexión establecida :) ")
    print("=================================")
    
except snowflake.connector.errors.ProgrammingError as e:
    print()
    print("==================================================================")
    print(f"Error. No se ha podido establecer la conexión: {e}")
    print("==================================================================")
"""    
# Ejecutamos una consulta
cur.execute("SELECT * FROM Region")

# Buscar resultados
rows = cur.fetchall()
for row in rows:
    print(row)
"""

print()
print("=================================")
print("Mostrando DataFrame: ")
print("=================================")
print(df)