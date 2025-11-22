-- Databricks notebook source
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
USING DELTA
LOCATION 'gs://dmc_bigdata_jmsp_052025/bronze/sap/personas/';

-- COMMAND ----------

select * from bronze.persona where id=27
