import requests # - Hacer solicitudes HTTP y descargar el contenido
from bs4 import BeautifulSoup # - Analizar el contenido HTML
import csv
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine
import snowflake.connector
import os

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

print()
print("--------------------------------")
print("Tabla guardada en archivo .CSV")
print("--------------------------------")
print()


# -------------------------------------- DATA LOADING --------------------------------------

# -- Configuramos la conexión a la base de datos en Snowflake

print()
print("--------------------------------")
print("Conectando a la base de datos...: ")
print("--------------------------------")

try:
    
    # Parámetros de la conexión
    account = 'te45066.switzerland-north'
    user = 'ALEJANDRO'   
    password = os.environ.get('SNOWFLAKE_PASSWORD')
    warehouse = 'PIPELINEWAREHOUSE'
    database = 'PIPLINER'
    schema = 'pipelineschema'
    
    # Conectar a Snowflake
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
    
    print()
    print("--------------------------------")
    print("Conexión establecida :) ")
    print("--------------------------------")
    
except snowflake.connector.errors.ProgrammingError as e:
    print()
    print("--------------------------------")
    print(f"Error. No se ha podido establecer la conexión: {e}")
    print("--------------------------------")
"""    
# Ejecutamos una consulta
cur.execute("SELECT * FROM Region")

# Buscar resultados
rows = cur.fetchall()
for row in rows:
    print(row)
"""

print()
print("--------------------------------")
print("Mostrando DataFrame: ")
print("--------------------------------")
print(df)