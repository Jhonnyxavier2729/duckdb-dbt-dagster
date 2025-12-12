-- Mart: Distribución por ciudad
WITH base_data AS (
    SELECT DISTINCT
        documento,
        ciudad
    FROM {{ ref('ref_victimas_cleaned') }}
)

SELECT 
    'Personas por Ciudad: ' || COALESCE(ciudad, 'Sin Información') AS metrica,
    COUNT(documento) AS valor
FROM base_data
GROUP BY ciudad
