# Databricks notebook source
# MAGIC %md
# MAGIC #### breweries_aggregated - load table

# COMMAND ----------

# Importar funções necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


# COMMAND ----------

# Inicializar Spark Session
spark = SparkSession.builder.appName("BreweryETL").getOrCreate()

# COMMAND ----------

# Carregar dados da camada Silver
silver_path = "/dbfs/tmp/silver/"
silver_df = spark.read.parquet(silver_path)

# COMMAND ----------

# Agregar dados
gold_df = silver_df.groupBy("state", "city", "brewery_type").count()
# Caminho para salvar os dados da camada Gold como tabela Delta
gold_path = "/dbfs/tmp/gold/breweries_aggregated"

# COMMAND ----------

# Outra forma para criar a tabela gold, passando o schema .
# # Leia os dados existentes
# gold_df = spark.read.format("delta").load(gold_path)

# # Defina o novo schema
# new_schema = StructType([
#     StructField("state", StringType(), True),
#     StructField("city", StringType(), True),
#     StructField("brewery_type", StringType(), True),
#     StructField("count", LongType(), False)  # False significa que não pode ser nulo
# ])

# # Aplique o novo schema
# gold_df_new = gold_df.select(
#     col("state").cast("string").alias("state"),
#     col("city").cast("string").alias("city"),
#     col("brewery_type").cast("string").alias("brewery_type"),
#     col("count").cast("long").alias("count")
# )

# # Você pode precisar cortar strings para garantir que se encaixam nas restrições de tamanho
# from pyspark.sql.functions import substring

# gold_df_final = gold_df_new.select(
#     substring(col("state"), 1, 50).alias("state"),
#     substring(col("city"), 1, 160).alias("city"),
#     substring(col("brewery_type"), 1, 50).alias("brewery_type"),
#     col("count")
# )

# # Sobrescreva ou salve os dados transformados
# gold_df_final.write.format("delta").mode("overwrite").save(gold_path)

# COMMAND ----------

# Salvar dados agregados como tabela Delta
gold_df.write.format("delta").mode("overwrite").save(gold_path)

# COMMAND ----------

# Registrar a tabela Delta no catálogo de metadados
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS breweries_aggregated
    USING DELTA
    LOCATION '{gold_path}'
""")

# COMMAND ----------

# Exibir os dados para garantir que foram carregados corretamente
display(gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Validations

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from breweries_aggregated
# MAGIC order by count desc 
# MAGIC limit 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT brewery_type, COUNT(*) as count
# MAGIC FROM breweries_aggregated
# MAGIC GROUP BY brewery_type
# MAGIC ORDER BY count DESC;
