
-- Mart: Distribución por grupos de edad

WITH base_data AS (
    SELECT DISTINCT
        documento,
        fecha_nacimiento
    FROM {{ ref('ref_victimas_cleaned') }}
),

edad_calculada AS (
    SELECT 
        documento,
        CASE 
            WHEN fecha_nacimiento IS NOT NULL THEN
                CASE 
                    WHEN DATE_DIFF('year', fecha_nacimiento, CURRENT_DATE) < 18 THEN 'Menor de edad'
                    WHEN DATE_DIFF('year', fecha_nacimiento, CURRENT_DATE) BETWEEN 18 AND 30 THEN '18-30 años'
                    WHEN DATE_DIFF('year', fecha_nacimiento, CURRENT_DATE) BETWEEN 31 AND 45 THEN '31-45 años'
                    WHEN DATE_DIFF('year', fecha_nacimiento, CURRENT_DATE) BETWEEN 46 AND 60 THEN '46-60 años'
                    WHEN DATE_DIFF('year', fecha_nacimiento, CURRENT_DATE) > 60 THEN 'Mayor de 60'
                    ELSE 'Sin Información'
                END
            ELSE 'Sin Información'
        END AS grupo_edad
    FROM base_data
)

SELECT 
    'Personas por Grupo de Edad: ' || grupo_edad AS metrica,
    COUNT(documento) AS valor
FROM edad_calculada
GROUP BY grupo_edad
ORDER BY valor DESC

