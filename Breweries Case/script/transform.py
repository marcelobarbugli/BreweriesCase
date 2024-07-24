# Databricks notebook source
# MAGIC %md
# MAGIC #### Transform - json to parquet

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Ler dados do Bronze Layer
bronze_path = "/dbfs/tmp/bronze/"
bronze_df = spark.read.json(bronze_path)

# COMMAND ----------

# Realizar transformações
silver_df = bronze_df.select("id", "name", "brewery_type", "city", "state", "country") \
    .filter(col("country").isNotNull())

# COMMAND ----------


# Salvar dados transformados no Silver Layer particionado por localização
silver_path = "/dbfs/tmp/silver/"
silver_df.write.mode("overwrite").partitionBy("state", "city").parquet(silver_path)

# Exibir os dados para garantir que foram carregados corretamente
display(silver_df)


# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /dbfs/tmp/silver/
