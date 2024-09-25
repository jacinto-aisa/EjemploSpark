# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Bronce;
# MAGIC DROP TABLE IF EXISTS bronce.salesorderdetail
# MAGIC
# MAGIC

# COMMAND ----------

run 

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
print (variable)
sc = SparkSession.builder.appName("ReadCSV").getOrCreate()

salesHeader_csv = "dbfs:/FileStore/SalesHeader.csv"
salesHeader_parquet = "dbfs:/FileStore/Bronce/SalesHeader"
salesHeader_df = sc.read.option("delimiter", ";").option("header",True).option("inferSchema","True").csv(salesHeader_csv)

salesHeader_df = salesHeader_df.select \
 ( \
    salesHeader_df.SalesOrderID.alias("OrderID"), \
    salesHeader_df.OrderDate.cast(DateType()).alias("FechaPedido_"), \
    salesHeader_df.DueDate.cast(DateType()).alias("FechaVenc_"), \
    salesHeader_df.ShipDate.cast(DateType()).alias("FechaEnv_"), \
    salesHeader_df.Status, \
    salesHeader_df.CustomerID, \
    salesHeader_df.TerritoryID, \
    salesHeader_df.ShipMethodID
 )

salesHeader_df = salesHeader_df.withColumn("FechaPedido",( \
    year(salesHeader_df.FechaPedido_)*10000 + \
    month(salesHeader_df.FechaPedido_)*100 + \
    day(salesHeader_df.FechaPedido_)))

salesHeader_df = salesHeader_df.withColumn("FechaVenc",( \
    year(salesHeader_df.FechaVenc_)*10000 + \
    month(salesHeader_df.FechaVenc_)*100 + \
    day(salesHeader_df.FechaVenc_)))

salesHeader_df = salesHeader_df.withColumn("FechaEnv",( \
    year(salesHeader_df.FechaEnv_)*10000 + \
    month(salesHeader_df.FechaEnv_)*100 + \
    day(salesHeader_df.FechaEnv_)))

salesHeader_df = salesHeader_df \
        .fillna({ \
            "CustomerID" : -1, \
            "TerritoryID" : -1
         })
#salesHeader_df.printSchema()


columns_to_drop = ['FechaPedido_','FechaVenc_','FechaEnv_']
salesHeader_df = salesHeader_df.drop(*columns_to_drop)

salesHeader_df.write.saveAsTable("bronce.salesorderdetail")

display(salesHeader_df)
