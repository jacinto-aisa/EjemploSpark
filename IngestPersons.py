# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Bronce;
# MAGIC DROP TABLE IF EXISTS bronce.person

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("ReadJson").getOrCreate()

person_json = "dbfs:/FileStore/Person.json"
person_parquet = "dbfs:/FileStore/Bronce/Person"
person_schema = StructType(fields=[ \
        StructField("BusinessEntityID", IntegerType(),False), \
        StructField("PersonType", StringType()), \
        StructField("NameStyle", StringType()), \
        StructField("Title", StringType()), \
        StructField("FirstName", StringType()), \
        StructField("MiddleName", StringType()), \
        StructField("LastName", StringType()), \
        StructField("Suffix", StringType()), \
        StructField("EmailPromotion", IntegerType()), \
        StructField("AdditionalContactInfo", StringType()), \
        StructField("Demographics", StringType()), \
        StructField("rowguid",StringType()), \
        StructField("ModifiedDate",DateType()) \
        ])

person_df = spark.read \
.schema(person_schema) \
.option("multiLine",True) \
.json(person_json)

#person_df.printSchema()
#person_df.display()

#seleccionar ñp que necesitamos.

person_df = person_df.select ( \
    person_df.BusinessEntityID.alias("PersonID"), \
    person_df.PersonType, \
    concat( \
        when(person_df.Title.isNull(), lit(" ")).otherwise(person_df.Title), \
        when(person_df.FirstName.isNull() , lit(" ")).otherwise(concat(person_df.FirstName,lit(" "))),\
        when(person_df.MiddleName.isNull(), lit(" ")).otherwise(concat(person_df.MiddleName,lit(", "))), \
        when(person_df.LastName.isNull(), lit(" ")).otherwise(person_df.LastName)
        ).alias("Nombre") 
        )

columnas = ['PersonID', 'PersonType','Nombre']
newRow = spark.createDataFrame( \
    [ \
    (-1, "","Persona No Informada"), \
    (-2, "","Persona No Encontrada") \
    ], columnas)
person_df = person_df.union(newRow)
display(person_df)
person_df.write.saveAsTable("bronce.person")

