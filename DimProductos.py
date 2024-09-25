# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Plata;
# MAGIC DROP TABLE IF EXISTS plata.dimproducto

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkSession.builder.config("k2", "v2").getOrCreate()

product_df = sc.sql ("SELECT * FROM bronce.products")
subcategory_df= sc.sql ("SELECT * FROM bronce.subcategory")
category_df = sc.sql("SELECT * FROM bronce.category")

productWithSubcategory_df = product_df.join(subcategory_df, product_df.SubCatID == subcategory_df.SubCategoryID, "leftouter")
productWithSubcategory_df.createOrReplaceTempView("productWithSub")
#Vamos a ver los que no coinciden con subcategoria, para que no se pierda la información dde estos productos
#vamos a poner como código de suncategoria la -2
productWithSubcategory_df = sc.sql(" \
SELECT ProductID, NombreProducto, PrecioCatalogo,Tamano,Peso,Clase,Estilo, \
    CASE WHEN SubCategoryID IS NULL THEN '-2' ELSE SubCatID END AS SubCatID, \
    Color,EnProd, \
    CASE WHEN SubCategorYID IS NULL THEN '-2' ELSE CategoryID END AS CategoryID, \
    CASE WHEN SubCategoryID IS NULL THEN 'SubCategoria No Encontrada' ELSE NombreSubCategoria END AS NombreSubCategoria \
FROM productWithSub ")
complete_df = productWithSubcategory_df.join(category_df, productWithSubcategory_df.CategoryID == category_df.CategoryID, "leftouter")
columns_to_drop = ['SubCatID','CategoryID']
complete_df = complete_df.drop(*columns_to_drop)
complete_df.write.saveAsTable("plata.dimproducto")
display(complete_df)

