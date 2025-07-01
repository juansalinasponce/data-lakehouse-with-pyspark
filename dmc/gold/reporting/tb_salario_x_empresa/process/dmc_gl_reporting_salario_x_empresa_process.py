# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

bucket = "dmc_bigdata_jmsp_052025" 
silver_layer = "silver" 
domain = "reporting"
path_silver = f'gs://{bucket}/{silver_layer}'
table_sl_sap_personas = f'{path_silver}/sap/personas'
table_sl_sap_empresas = f'{path_silver}/sap/empresa'
table_gl_reporting_salario_x_empresa = f"gs://{bucket}/gold/reporting/salario_x_empresa"
print(table_gl_reporting_salario_x_empresa)

# COMMAND ----------

df_personas = spark.read.format("delta").load(table_sl_sap_personas)
df_empresas = spark.read.format("delta").load(table_sl_sap_empresas)

# COMMAND ----------

df_personas.createOrReplaceTempView("personas")

# COMMAND ----------

display(df_personas)

# COMMAND ----------

df_empresas.createOrReplaceTempView("empresas")

# COMMAND ----------

df_empresas.createOrReplaceGlobalTempView("empresas2")

# COMMAND ----------

display(df_empresas)

# COMMAND ----------

query="Select p.NOMBRE,p.salario,e.empresa_name as nombre_empresa from personas p inner join empresas e on p.id_empresa = e.id and p.periodo=e.periodo"
df_result = spark.sql(query)

# COMMAND ----------

df_result.write.mode("overwrite").format("delta").save(table_gl_reporting_salario_x_empresa)


# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gold.kpi_empleados
# MAGIC Select   p.periodo,
# MAGIC                     e.empresa_name as empresa,
# MAGIC                     p.edad_categoria, 
# MAGIC                     avg(p.edad) as prom_edad,
# MAGIC                     avg(p.salario) as prom_salario,
# MAGIC                     sum(p.salario) as planilla,
# MAGIC                     count(1) as num_empleados,
# MAGIC                     max(p.salario) as max_salario,
# MAGIC                     min(p.salario) as min_salario
# MAGIC             from silver.personas p
# MAGIC         inner join silver.empresas e on e.id = p.id_empresa 
# MAGIC         and e.periodo = p.periodo
# MAGIC         group by p.periodo,e.empresa_name,p.edad_categoria 
# MAGIC         order by p.periodo,e.empresa_name,p.edad_categoria
