# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import current_date,date_format

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

#Variables
bucket = "dmc_bigdata_jmsp_052025"
bronze_layer = "bronze" 
silver_layer = "silver" 
system_source = "sap" 
table = "empresa"
path_source = f'gs://{bucket}/{bronze_layer}/{system_source}/{table}/'
path_target = f'gs://{bucket}/{silver_layer}/{system_source}/{table}/'

# COMMAND ----------

df = spark.read.format("delta").load(path_source)



# COMMAND ----------

#transformacione 
# 
df_t = df.withColumn('ID',col('ID').cast(IntegerType()))\
    .withColumn('fecha_proceso', current_date())\
    .withColumn('periodo', date_format(current_date(),'yyyyMM'))


# COMMAND ----------

df_t.write.mode("overwrite").format("delta").partitionBy('periodo').save(path_target)

# COMMAND ----------

df2 = spark.read.format("delta").load(path_target)
display(df2)