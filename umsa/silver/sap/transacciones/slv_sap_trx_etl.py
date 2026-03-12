# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType,DoubleType
from pyspark.sql.functions import date_format,col,year,month,dayofmonth,to_date,current_date

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

table = "transacciones"
table_source = f"bronze.{table}"
table_target = f"silver.{table}"
print(table_source,table_target)

# COMMAND ----------

df = spark.table(table_source)
df.show()

# COMMAND ----------

df_t = df.withColumn('id_persona',col('id_persona').cast(IntegerType()))\
    .withColumn('id_empresa',col('id_empresa').cast(IntegerType()))\
    .withColumn('monto', col('monto').cast(DoubleType()))\
    .withColumn('fecha', to_date(col('fecha'), 'yyyy-MM-dd'))\
    .withColumn('anio', year(col('fecha')))\
    .withColumn('mes', month(col('fecha')))\
    .withColumn('dia', dayofmonth(col('fecha')))\
    .withColumn('PERIODO', date_format(current_date(),'yyyyMM'))

# COMMAND ----------

display(df_t)


# COMMAND ----------

df_t.write.mode("overwrite").format("delta").partitionBy('PERIODO').saveAsTable(table_target)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.transacciones