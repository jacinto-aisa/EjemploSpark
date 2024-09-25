# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Bronce;
# MAGIC DROP TABLE IF EXISTS bronce.subcategory

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadXML").getOrCreate()

subCategory_json = "dbfs:/FileStore/ProductSubCategory.json"
subCategory_parquet = "dbfs:/FileStore/Bronce/SubCategory"
subCategory_schema = StructType(fields=[
        StructField("ProductSubcategoryID", StringType(),False),
        StructField("ProductCategoryID", StringType()),
        StructField("Name", StringType()),
        StructField("rowguid",StringType()),
        StructField("ModifiedDate",StringType())
])

subCategory_df = spark.read \
.schema(subCategory_schema) \
.option("multiLine",True) \
.json(subCategory_json)

#subCategory_df.printSchema()
#subCategory_df.show(10)

#seleccionar lo que necesitamos.

subCategory_df = subCategory_df.select \
 ( \
     col("ProductSubcategoryID").alias("SubCategoryID"), \
     col("ProductCategoryID").alias("CategoryID"), \
     col("Name").alias("NombreSubCategoria")
 )

subCategory_df = subCategory_df \
        .fillna({ \
            "CategoryID" : -1 
         })
columnas = ['SubCategoryID', 'CategoryID', 'NombreSubCategoria']
newRow = spark.createDataFrame([('-1', '-1', "SubCategoria No Informada")], columnas)
subCategory_df = subCategory_df.union(newRow)
newRow = spark.createDataFrame([('-2', '-2', "SubCategoria No Encontrada")], columnas)
subCategory_df = subCategory_df.union(newRow)
#subCategory_df.printSchema()
display(subCategory_df)
subCategory_df.write.saveAsTable("bronce.subcategory")
