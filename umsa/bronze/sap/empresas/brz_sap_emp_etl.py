# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

path = "/Volumes/workspace/datalake/landing"
source = "sap"
table = "empresas"
file_name = "empresa.data"
table_name = "bronze.empresa"
path_source = f"{path}/{source}/{table}/{file_name}"

# COMMAND ----------

df_schema = StructType([
StructField("ID", StringType(),True),
StructField("NOMBRE", StringType(),True)
])

# COMMAND ----------

df = spark.read.format("CSV").option("header","true").option("delimiter","|").schema(df_schema).load(path_source)
df.show()


# COMMAND ----------

df.write.mode("overwrite").saveAsTable(table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze.empresa