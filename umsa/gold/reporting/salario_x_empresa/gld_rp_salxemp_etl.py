# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

#Tablas requeridad silver y/o gold
silver_personas = "silver.persona"
silver_empresas = "silver.empresa"
df_persona = spark.table(silver_personas)
df_empresa = spark.table(silver_empresas)
display(df_persona)
display(df_empresa)


# COMMAND ----------

df_persona.createOrReplaceTempView("persona")
df_empresa.createOrReplaceTempView("empresa")


# COMMAND ----------

df_t = spark.sql("""
           Select   p.periodo,
                     e.NOMBRE as empresa,
                     p.edad_categoria, 
                     avg(p.edad) as prom_edad,
                     avg(p.salario) as prom_salario,
                     sum(p.salario) as planilla,
                     count(1) as num_empleados,
                     max(p.salario) as max_salario,
                     min(p.salario) as min_salario
             from persona p
         inner join empresa e on e.id = p.id_empresa 
         and e.periodo = p.periodo
         group by p.periodo,e.NOMBRE,p.edad_categoria 
          """)



# COMMAND ----------

df_t.write.mode("overwrite").format("delta").partitionBy('PERIODO').saveAsTable("gold.kpi_empleados")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.kpi_empleados