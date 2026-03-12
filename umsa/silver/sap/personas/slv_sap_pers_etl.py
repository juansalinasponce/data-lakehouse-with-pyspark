# Databricks notebook source

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,DoubleType
from pyspark.sql.functions import regexp_replace,col,to_date,when,current_date,date_format,dayofmonth,year,month

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

table = "persona"
table_source = f"brz.{table}"
table_target = f"slv.{table}"
print(table_source,table_target)

# COMMAND ----------

df = spark.table(table_source)
df.show()

# COMMAND ----------

df_t = df.withColumn('telefono', regexp_replace('telefono', '-', ''))\
    .withColumn('ID',col('ID').cast(IntegerType()))\
    .withColumn('id_empresa',col('id_empresa').cast(IntegerType()))\
    .withColumn('salario',col('salario').cast(DoubleType()))\
    .withColumn('edad',col('edad').cast(IntegerType()))\
    .withColumn("fecha_ingreso", to_date ("fecha_ingreso","yyyy-MM-dd"))\
    .withColumn(
        'edad_categoria',
        when(col('edad') < 30, 'Joven')
        .when((col('edad') >= 30) & (col('edad') <= 50), 'Adulto')
        .otherwise('Adulto mayor')
    )\
    .withColumn('fecha_proceso', current_date())\
    .withColumn('periodo', date_format(current_date(),'yyyyMM'))\
    .withColumn('anio',year(current_date()))\
    .withColumn('mes',month(current_date()))\
    .withColumn('dia',dayofmonth(current_date()))\
    .withColumn('dia_texto', date_format(current_date(), 'EEEE'))

# COMMAND ----------

display(df_t)


# COMMAND ----------

df_t.write.mode("overwrite").format("delta").partitionBy('anio', 'mes', 'dia').saveAsTable(table_target)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.persona