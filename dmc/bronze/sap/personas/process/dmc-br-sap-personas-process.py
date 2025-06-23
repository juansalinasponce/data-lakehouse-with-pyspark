# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

#Variables
bucket = "dmc_bigdata_jmsp_052025"
landing_layer = "landing" 
bronze_layer = "bronze" 
system_source = "sap" 
table = "personas"
file_name = "persona.data"
path_source = f'gs://{bucket}/{landing_layer}/{system_source}/{table}/{file_name}'
path_target = f'gs://{bucket}/{bronze_layer}/{system_source}/{table}/'
print(path_target)

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



# COMMAND ----------

df.write.mode("overwrite").format("delta").save(path_target)