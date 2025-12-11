-- Mart: Personas con discapacidad

WITH base_data AS (
    SELECT DISTINCT
        documento,
        discapacidad
    FROM {{ ref('ref_victimas_cleaned') }}
)

SELECT 
    CASE 
        WHEN discapacidad = 1 THEN 'Con Discapacidad'
        ELSE 'Sin Discapacidad'
    END AS metrica,
    COUNT(documento) AS valor
FROM base_data
GROUP BY discapacidad
