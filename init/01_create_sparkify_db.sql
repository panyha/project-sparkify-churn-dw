-- Runs once, the first time the Postgres volume is created.
CREATE DATABASE sparkify_dw;
\connect sparkify_dw
CREATE SCHEMA IF NOT EXISTS raw;
