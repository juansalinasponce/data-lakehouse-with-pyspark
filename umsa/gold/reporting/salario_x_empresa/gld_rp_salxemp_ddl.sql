-- Databricks notebook source
create database gold;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS gold.kpi_empleados ( 
  periodo STRING, 
  empresa STRING, 
  edad_categoria STRING, 
  prom_edad DOUBLE, 
  prom_salario DOUBLE, 
  planilla DOUBLE, 
  num_empleados BIGINT, 
  max_salario DOUBLE, 
  min_salario DOUBLE ) 
  USING DELTA 
  PARTITIONED BY (periodo) COMMENT 'KPI de empleados por periodo, empresa y categoría de edad';