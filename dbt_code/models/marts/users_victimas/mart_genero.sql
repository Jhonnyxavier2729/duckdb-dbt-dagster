-- Mart: Distribución por género
WITH base_data AS (
    SELECT DISTINCT
        documento,
        genero
    FROM {{ ref('ref_victimas_cleaned') }}
)

SELECT 
    'Total Registrados' AS metrica,
    COUNT(documento) AS valor
FROM base_data

UNION ALL

SELECT 
    'Personas por Género: ' || COALESCE(genero, 'Sin Información') AS metrica,
    COUNT(documento) AS valor
FROM base_data
GROUP BY genero
ORDER BY valor DESC
