{{ config(materialized='view') }}

-- Referencia a tabla raw cargada por Dagster
SELECT * FROM raw.victimas
