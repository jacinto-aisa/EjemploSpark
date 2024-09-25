# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS Oro;
# MAGIC DROP TABLE IF EXISTS oro.vterritoryid

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkSession.builder.appName("Ventas").getOrCreate()

#vamos a cargar en tablas temporales y luego haremos SQL para extraer la información

dim_clientes_df = sc.sql("SELECT * FROM plata.dimcliente")
dim_fecha_df = sc.sql("SELECT * FROM plata.dimfecha")
fact_ventas_df = sc.sql("SELECT * FROM plata.factventas")

#tomamos la información que necesitemos.
fact_ventas_df = fact_ventas_df.select \
( \
    fact_ventas_df.TotalLinea, \
    fact_ventas_df.TerritoryID, \
    fact_ventas_df.FechaPedido \
)

#primer join, fact_ventas con Fecha
fact_ventas_df = fact_ventas_df.join(dim_fecha_df,fact_ventas_df.FechaPedido == dim_fecha_df.dateInt)

columns_to_drop = ['FechaPedido','dateInt','CalendarDate','NombreDia','DiaDeLaSemana','DiaDelMes','Dia','Semana','Trimestre','EsUltimoDiaDeMes']
fact_ventas_df = fact_ventas_df.drop(*columns_to_drop)

#vamos a agrupar por código de Cliente
#fact_ventas_df.show(10)
fact_ventas_df.createOrReplaceTempView("FactVentas")
fact_ventas_df = sc.sql("SELECT TerritoryID,Ano,NombreMes,Mes,SUM(TotalLinea) as TotatCliente \
                         FROM FactVentas \
                         GROUP BY TerritoryID,Ano,NombreMes,Mes ")
fact_ventas_df.write.saveAsTable("oro.vterritoryid")
display(fact_ventas_df)
