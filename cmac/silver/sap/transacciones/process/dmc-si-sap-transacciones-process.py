# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,DoubleType
from pyspark.sql.functions import date_format,col,year,month,dayofmonth,to_date

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

#Variables
bucket = "dmc_bigdata_jmsp_052025"
bronze_layer = "bronze" 
silver_layer = "silver" 
system_source = "sap" 
table = "transacciones"
path_source = f'gs://{bucket}/{bronze_layer}/{system_source}/{table}/'
path_target = f'gs://{bucket}/{silver_layer}/{system_source}/{table}/'

# COMMAND ----------

df = spark.read.format("delta").load(path_source)



# COMMAND ----------

display(df)

# COMMAND ----------

#transformacione 
# 
df_t = df.withColumn('id_persona',col('id_persona').cast(IntegerType()))\
    .withColumn('id_empresa',col('id_empresa').cast(IntegerType()))\
    .withColumn('monto', col('monto').cast(DoubleType()))\
    .withColumn('fecha', to_date(col('fecha'), 'yyyy-MM-dd'))\
    .withColumn('anio', year(col('fecha')))\
    .withColumn('mes', month(col('fecha')))\
    .withColumn('dia', dayofmonth(col('fecha')))


# COMMAND ----------

display(df_t)

# COMMAND ----------

df_t.write.mode("overwrite").format("delta").partitionBy('anio','mes','dia').save(path_target)

# COMMAND ----------

df2 = spark.read.format("delta").load(path_target)
display(df2)