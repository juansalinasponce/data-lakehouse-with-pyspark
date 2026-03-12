# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import current_date,date_format,col

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

table = "empresa"
table_source = f"bronze.{table}"
table_target = f"silver.{table}"
print(table_source,table_target)

# COMMAND ----------

df = spark.table(table_source)
df.show()

# COMMAND ----------

df_t = df.withColumn('ID',col('ID').cast(IntegerType()))\
    .withColumn('fecha_proceso', current_date())\
    .withColumn('PERIODO', date_format(current_date(),'yyyyMM'))

# COMMAND ----------

display(df_t)


# COMMAND ----------

df_t.write.mode("overwrite").format("delta").partitionBy('PERIODO').saveAsTable(table_target)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.empresa