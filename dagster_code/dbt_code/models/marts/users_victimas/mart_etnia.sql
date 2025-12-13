-- Mart: Distribución por pertenencia étnica
WITH base_data AS (
    SELECT DISTINCT
        documento,
        pertenencia_etnica
    FROM {{ ref('ref_victimas_cleaned') }}
)

SELECT 
    'Personas por Etnia: ' || COALESCE(pertenencia_etnica, 'Sin Información') AS metrica,
    COUNT(documento) AS valor
FROM base_data
GROUP BY pertenencia_etnica
