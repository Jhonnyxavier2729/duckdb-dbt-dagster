{{
    config(
        materialized='incremental',
        unique_key='documento',
        on_schema_change='fail'
    )
}}

-- Refined: clean data without duplicates
-- Uses incremental strategy to handle repeated data loads
WITH source AS (
    SELECT * FROM {{ ref('int_victimas_intermediate') }}
),

deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY documento 
            ORDER BY fecha_reporte DESC NULLS LAST
        ) AS row_num
    FROM source
    {% if is_incremental() %}
    -- Only process new or updated records on incremental runs
    WHERE fecha_reporte >= (SELECT MAX(fecha_reporte) FROM {{ this }})
    {% endif %}
)

SELECT 
    * EXCLUDE (row_num)
FROM deduped
WHERE row_num = 1
