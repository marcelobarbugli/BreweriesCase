# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Fetch Data from the Open Brewery DB API
# MAGIC

# COMMAND ----------

import requests
import json
import zipfile
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

# Inicializar Spark Session
spark = SparkSession.builder.appName("BreweryETL").getOrCreate()

# COMMAND ----------

# Passo 1: Buscar dados da API
try:
    response = requests.get("https://api.openbrewerydb.org/breweries")
    response.raise_for_status()  # Levantar exceção para erros de resposta
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"Erro ao buscar dados da API: {e}")
    data = None

# COMMAND ----------

# Passo 2: Definir esquema e criar DataFrame
if data:
    try:
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("country", StringType(), True),
            StructField("longitude", StringType(), True),
            StructField("latitude", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True)
        ])

        raw_df = spark.createDataFrame(data, schema=schema)
    except Exception as e:
        print(f"Erro ao criar DataFrame: {e}")
        raw_df = None

# COMMAND ----------

# Passo 3: Salvar DataFrame no DBFS
if raw_df:
    try:
        bronze_path = "/dbfs/tmp/bronze/"
        raw_df.write.mode("overwrite").json(bronze_path)
    except Exception as e:
        print(f"Erro ao salvar DataFrame no DBFS: {e}")

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /dbfs/tmp/bronze/

# COMMAND ----------

# Passo 4: Copiar arquivo do DBFS para o local
try:
    local_bronze_path = "/tmp/bronze/"
    dbutils.fs.cp(bronze_path, f"file://{local_bronze_path}", recurse=True)
except Exception as e:
    print(f"Erro ao copiar arquivo do DBFS: {e}")
