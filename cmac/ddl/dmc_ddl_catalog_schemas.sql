-- Databricks notebook source
show databases;

-- COMMAND ----------

show catalogs;

-- COMMAND ----------

show tables in default;

-- COMMAND ----------



-- COMMAND ----------

create schema bronze location "gs://dmc_bigdata_jmsp_052025/bronze/"

-- COMMAND ----------

create schema silver location "gs://dmc_bigdata_jmsp_052025/silver/"

-- COMMAND ----------

create schema if not exists gold location "gs://dmc_bigdata_jmsp_052025/gold/"