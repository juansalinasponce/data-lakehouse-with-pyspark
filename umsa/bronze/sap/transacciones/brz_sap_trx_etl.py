# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

path = "/Volumes/workspace/datalake/landing"
source = "sap"
table = "transacciones"
file_name = "transacciones.data"
table_name = "bronze.transacciones"
path_source = f"{path}/{source}/{table}/{file_name}"

# COMMAND ----------

df_schema = StructType([
StructField("ID_PERSONA", StringType(),True),
StructField("ID_EMPRESA", StringType(),True),
StructField("MONTO", StringType(),True),
StructField("FECHA", StringType(),True)
])


# COMMAND ----------

df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(path_source)
df.show()


# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable(table_name)workspace.bronze.transaccionesworkspace.bronze.transacciones