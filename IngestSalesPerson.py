# Databricks notebook source
# MAGIC %md
# MAGIC Por si hemos creado una carpeta fichero parquet y no podemos borrarlo.
# MAGIC

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *



# COMMAND ----------

dbutils.help


# COMMAND ----------

# MAGIC %md
# MAGIC #Conexión mediante SAS
# MAGIC - Cuenta de almacenamiento:
# MAGIC - Token SAS
# MAGIC

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.almacenaant.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.almacenaant.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.almacenaant.dfs.core.windows.net","?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-02-28T23:09:18Z&st=2024-01-16T15:09:18Z&spr=https&sig=lwj6tmJsl0y1NdgAeHGT5Qu3bSFPNrSWZhDtFTIYED8%3D")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Bronce;
# MAGIC DROP TABLE IF EXISTS bronce.salesperson

# COMMAND ----------

dbutils.fs.ls("abfss://raw@almacenaant.dfs.core.windows.net/")
sales_person_df = spark.read.option("delimiter", ";").option("header",True).option("inferSchema", \
    "True").csv(F"abfss://raw@almacenaant.dfs.core.windows.net/SalesPerson.csv")
display(sales_person_df)

# COMMAND ----------



dbutils.fs.ls("abfss://raw@almacenaant.dfs.core.windows.net/")


sales_person_df = spark.read.option("delimiter", ";").option("header",True).option("inferSchema", \
    "True").csv(F"abfss://raw@almacenaant.dfs.core.windows.net/SalesPerson.csv")

sales_person_df = sales_person_df.select \
 ( \
    sales_person_df.BusinessEntityID.alias('PersonID'), \
    sales_person_df.TerritoryID, \
    sales_person_df.SalesQuota, \
    sales_person_df.Bonus, \
    sales_person_df.CommissionPct, \
    sales_person_df.ModifiedDate.alias("FechaMod_") \
 )
sales_person_df = sales_person_df \
        .fillna({ \
            "PersonID" : -1, \
         })
sales_person_df = sales_person_df.withColumn("FechaMod",( \
    year(sales_person_df.FechaMod_)*10000 + \
    month(sales_person_df.FechaMod_)*100 + \
    day(sales_person_df.FechaMod_)))

columns_to_drop = ['FechaMod_']
sales_person_df = sales_person_df.drop(*columns_to_drop)

sales_person_df.write.saveAsTable("bronce.SalesPerson")
spark.sql("INSERT INTO bronce.salesperson (PersonId,TerritoryID,FechaMod) \
        VALUES (-1,-1,GETDATE()),(-2,-2,GETDATE())")

sales_person_df = spark.sql("SELECT * FROM bronce.salesperson")
display(sales_person_df)

