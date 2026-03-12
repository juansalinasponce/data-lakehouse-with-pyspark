-- Databricks notebook source
CREATE TABLE IF NOT EXISTS bronze.transacciones(
    ID_PERSONA STRING,
    ID_EMPRESA STRING,
    MONTO STRING,
    FECHA STRING
)