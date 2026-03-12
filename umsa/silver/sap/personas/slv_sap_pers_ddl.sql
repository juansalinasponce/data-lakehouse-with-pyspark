-- Databricks notebook source
show databases

-- COMMAND ----------

create schema silver;

-- COMMAND ----------

drop table silver.personas

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS silver.persona (
    ID INT,
    NOMBRE STRING,
    telefono STRING,
    correo STRING,
    fecha_ingreso DATE,
    edad INT,
    salario DOUBLE,
    id_empresa INT,
    edad_categoria STRING,
    fecha_proceso DATE,
    periodo STRING,
    anio int,
    mes int,
    dia int,
    dia_texto STRING
)
USING DELTA
PARTITIONED BY (periodo)
