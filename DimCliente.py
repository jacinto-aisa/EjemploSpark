# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Plata;
# MAGIC DROP TABLE IF EXISTS plata.dimcliente 

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkSession.builder.config("k2", "v2").getOrCreate()
customer_df =sc.sql("SELECT * FROM bronce.customer")
person_df=sc.sql("SELECT * FROM bronce.person")
person_df = person_df.withColumn("IDCliente",person_df.PersonID)
customerWithPerson_df = customer_df.join(person_df, customer_df.PersonID == person_df.IDCliente, "leftouter")
columns_to_drop = ['PersonID']
complete_df = customerWithPerson_df.drop(*columns_to_drop)
complete_df.write.saveAsTable("plata.dimcliente")
display(complete_df)

