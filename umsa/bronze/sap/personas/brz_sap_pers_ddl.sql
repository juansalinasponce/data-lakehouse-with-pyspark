-- Databricks notebook source
create schema bronze

-- COMMAND ----------

-- DBTITLE 1,Cell 2
CREATE TABLE IF NOT EXISTS bronze.persona(
    ID STRING,
    NOMBRE STRING,
    TELEFONO STRING,
    CORREO STRING,
    FECHA_INGRESO STRING,
    EDAD STRING,
    SALARIO STRING,
    ID_EMPRESA STRING
)




-- COMMAND ----------

select * from bronze.persona