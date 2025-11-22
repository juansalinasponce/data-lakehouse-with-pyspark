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
table = "transacciones"
file_name = "transacciones.data"
path_source = f'gs://{bucket}/{landing_layer}/{system_source}/{table}/{file_name}'
path_target = f'gs://{bucket}/{bronze_layer}/{system_source}/{table}/'

# COMMAND ----------

df_schema = StructType([
StructField("ID_PERSONA", StringType(),True),
StructField("ID_EMPRESA", StringType(),True),
StructField("MONTO", StringType(),True),
StructField("FECHA", StringType(),True)
])

# COMMAND ----------

df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(path_source)



# COMMAND ----------

df.write.mode("overwrite").format("delta").save(path_target)

# COMMAND ----------

df2 = spark.read.format("delta").load(path_target)

# COMMAND ----------

df2.count()

# COMMAND ----------

df2.count()