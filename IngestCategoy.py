# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Bronce;
# MAGIC DROP TABLE IF EXISTS bronce.category 

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

Category_csv = "dbfs:/FileStore/ProductsCategory.csv"
Category_df = spark.read.option("delimiter", ";").csv(Category_csv)

Category_df = Category_df.select \
 ( \
     col("_c0").alias("CategoryID"), \
     col("_c1").alias("NombreCategoria")
 )

Category_df.write.saveAsTable("bronce.category")
spark.sql("INSERT INTO bronce.category (CategoryId,NombreCategoria) VALUES (-1,'Categoria No Informada'),(-2,'Categoria No Encontrada')")
Category_df = spark.sql("SELECT * FROM bronce.category")
display(Category_df)


# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Importamos lo necesaario

# COMMAND ----------

# MAGIC %md
