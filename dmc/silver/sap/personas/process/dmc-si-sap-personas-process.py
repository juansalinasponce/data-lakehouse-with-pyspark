# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,IntegerType,DoubleType
from pyspark.sql.functions import regexp_replace,col,to_date,when,current_date,date_format,dayofmonth,year,month

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

#Variables
bucket = "dmc_bigdata_jmsp_052025"
bronze_layer = "bronze" 
silver_layer = "silver" 
system_source = "sap" 
table = "personas"
path_source = f'gs://{bucket}/{bronze_layer}/{system_source}/{table}/'
path_target = f'gs://{bucket}/{silver_layer}/{system_source}/{table}/'

# COMMAND ----------

df = spark.read.format("delta").load(path_source)



# COMMAND ----------

#transformacione 
# 
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

df_t.write.mode("overwrite").format("delta").partitionBy('anio', 'mes', 'dia').save(path_target)

# COMMAND ----------

df2 = spark.read.format("delta").load(path_target)
display(df2)