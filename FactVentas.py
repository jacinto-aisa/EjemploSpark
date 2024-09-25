# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkSession.builder.config("k2", "v2").getOrCreate()

header_df = sc.sql ("SELECT * FROM bronce.salesorderdetail")
detail_df= sc.sql ("SELECT * FROM bronce.salesdetail")

complete_df = detail_df.join(header_df, detail_df.SalesOrderID == header_df.OrderID, "leftouter")

#complete_df.filter(complete_df.Status.isNull()).show(10)
columns_to_drop = ['OrderID','FechaMod']
complete_df = complete_df.drop(*columns_to_drop)
complete_df.write.saveAsTable("plata.FactVentas")
display(complete_df)


