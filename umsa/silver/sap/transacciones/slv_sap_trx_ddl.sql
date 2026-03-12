-- Databricks notebook source
-- DBTITLE 1,Cell 1
DESCRIBE TABLE bronze.transacciones

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.transacciones (
    ID_PERSONA INT,
    ID_EMPRESA INT,
    MONTO DOUBLE,
    FECHA DATE,
    ANIO INT,
    MES INT,
    DIA INT,
    PERIODO STRING
)
USING DELTA
PARTITIONED BY (PERIODO)
