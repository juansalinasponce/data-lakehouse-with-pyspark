# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

path = "/Volumes/workspace/datalake/landing"
source = "sap"
table = "personas"
file_name = "persona.data"
table_name = "bronze.persona"
path_source = f"{path}/{source}/{table}/{file_name}"

# COMMAND ----------

df_schema = StructType([
StructField("ID", StringType(),True),
StructField("NOMBRE", StringType(),True),
StructField("TELEFONO", StringType(),True),
StructField("CORREO", StringType(),True),
StructField("FECHA_INGRESO", StringType(),True),
StructField("EDAD", StringType(),True),
StructField("SALARIO", StringType(),True),
StructField("ID_EMPRESA", StringType(),True),
])


# COMMAND ----------

df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(path_source)
df.show()

# COMMAND ----------

df.write.mode("overwrite").saveAsTable(table_name)