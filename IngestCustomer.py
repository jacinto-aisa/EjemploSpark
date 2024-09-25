# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Bronce;
# MAGIC DROP TABLE IF EXISTS bronce.customer 

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkSession.builder.appName("ReadCSV").getOrCreate()

customer_json = "dbfs:/FileStore/Customers.csv"
Customer_parquet = "dbfs:/FileStore/Bronce/Customers"
customer_df = sc.read.load(customer_json,format='com.databricks.spark.csv',header='true',inferSchema='true').cache()



customer_df = customer_df.select \
 ( \
    customer_df.CustomerID, \
    customer_df.PersonID.cast(IntegerType()).alias('PersonID'), \
    customer_df.TerritoryID \
 )

customer_df = customer_df \
        .fillna({ \
            "PersonID" : -1, \
            "TerritoryID" : -1
         })

columnas = ['CustomerID', 'PersonID', 'TerritoryID']
newRow = spark.createDataFrame([(-1, -1, -1),(-2, -2, -2,)], columnas)
customer_df = customer_df.union(newRow)

customer_df.display()
customer_df.write.saveAsTable("bronce.customer")
