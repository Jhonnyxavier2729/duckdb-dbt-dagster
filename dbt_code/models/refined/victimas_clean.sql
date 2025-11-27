{{ config(materialized='table') }}

-- Refined: datos limpios y sin duplicados
WITH deduped AS (
    SELECT DISTINCT *
    FROM {{ ref('int_victimas_cleaned') }}
)

SELECT * FROM deduped
