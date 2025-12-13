{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_victimas_start') }}
),

cleaned AS (
    SELECT
        -- Clean text: TRIM whitespace and convert empty strings to NULL
        NULLIF(TRIM(origen), '') AS origen,
        NULLIF(TRIM(fuente), '') AS fuente,
        NULLIF(TRIM(programa), '') AS programa,
        NULLIF(TRIM(UPPER(tipo_documento)), '') AS tipo_documento,  -- Uppercase for consistency
        
        -- Names 
        NULLIF(TRIM(primer_nombre), '') AS primer_nombre,
        NULLIF(TRIM(segundo_nombre), '') AS segundo_nombre,
        NULLIF(TRIM(primer_apellido), '') AS primer_apellido,
        NULLIF(TRIM(segundo_apellido), '') AS segundo_apellido,
        NULLIF(TRIM(nombre_completo), '') AS nombre_completo,
        
        -- Dates (already casted in staging)
        fecha_nacimiento,
        fecha_expedicion_documento,
        fecha_ocurrencia,
        fecha_reporte,
        fecha_valoracion,
        
        -- Text fields
        NULLIF(TRIM(expedicion_documento), '') AS expedicion_documento,
        NULLIF(TRIM(pertenencia_etnica), '') AS pertenencia_etnica,
        NULLIF(TRIM(UPPER(genero)), '') AS genero,  -- Uppercase for consistency
        NULLIF(TRIM(tipo_hecho), '') AS tipo_hecho,
        NULLIF(TRIM(hecho), '') AS hecho,
        
        -- IDs (already casted in staging)
        id_persona,
        id_hogar,
        documento,
        cod_dane_municipio_ocurrencia,
        cod_dane_municipio_residencia,
        cod_dane_declaracion,
        cod_dane_llegada,
        id_siniestro,
        id_mi_jefe,
        codigo_hecho,
        cons_persona,
        discapacidad,
        registraduria,
        
        -- Locations
        NULLIF(TRIM(zona_ocurrencia), '') AS zona_ocurrencia,
        NULLIF(TRIM(ubicacion_ocurrencia), '') AS ubicacion_ocurrencia,
        NULLIF(TRIM(zona_residencia), '') AS zona_residencia,
        NULLIF(TRIM(ubicacion_residencia), '') AS ubicacion_residencia,
        NULLIF(TRIM(direccion), '') AS direccion,
        NULLIF(TRIM(pais), '') AS pais,
        NULLIF(TRIM(ciudad), '') AS ciudad,
        
        -- Actors
        NULLIF(TRIM(presunto_actor), '') AS presunto_actor,
        NULLIF(TRIM(presunto_victimizante), '') AS presunto_victimizante,
        
        -- Types
        NULLIF(TRIM(tipo_poblacion), '') AS tipo_poblacion,
        NULLIF(TRIM(tipo_victima), '') AS tipo_victima,
        NULLIF(TRIM(tipo_desplazamiento), '') AS tipo_desplazamiento,
        
        -- Contact
        NULLIF(TRIM(num_telefono_fijo), '') AS num_telefono_fijo,
        NULLIF(TRIM(num_telefono_celular), '') AS num_telefono_celular,
        NULLIF(TRIM(email), '') AS email,
        
        -- Status and others
        NULLIF(TRIM(estado_victima), '') AS estado_victima,
        NULLIF(TRIM(vigencia_documento), '') AS vigencia_documento,
        NULLIF(TRIM(relacion), '') AS relacion,
        
        -- Disability
        NULLIF(TRIM(descripcion_discapacidad), '') AS descripcion_discapacidad,
        
        -- Others
        NULLIF(TRIM(fud_ficha), '') AS fud_ficha,
        NULLIF(TRIM(afectaciones), '') AS afectaciones
        
    FROM source_data
)

SELECT * FROM cleaned
