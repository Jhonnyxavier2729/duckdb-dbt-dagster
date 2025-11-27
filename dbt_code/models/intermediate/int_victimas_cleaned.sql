{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM {{ ref('stg_victimas') }}
),

cleaned AS (
    SELECT
        -- Remover espacios y normalizar texto
        TRIM(LOWER(ORIGEN)) AS origen,
        TRIM(LOWER(FUENTE)) AS fuente,
        TRIM(LOWER(PROGRAMA)) AS programa,
        
        
        
        -- Documentos
        TRIM(UPPER(TIPODOCUMENTO)) AS tipo_documento,
        
        
        -- Nombres 
        TRIM(LOWER(PRIMERNOMBRE)) AS primer_nombre,
        TRIM(LOWER(SEGUNDONOMBRE)) AS segundo_nombre,
        TRIM(LOWER(PRIMERAPELLIDO)) AS primer_apellido,
        TRIM(LOWER(SEGUNDOAPELLIDO)) AS segundo_apellido,
        TRIM(LOWER(NOMBRECOMPLETO)) AS nombre_completo,
        
        -- Fechas: convertir desde DD/MM/YYYY a formato ISO (YYYY-MM-DD)
        TRY_STRPTIME(TRIM(FECHANACIMIENTO), '%d/%m/%Y')::DATE AS fecha_nacimiento,
        TRY_STRPTIME(TRIM(FECHAEXPEDICIONDOCUMENTO), '%d/%m/%Y')::DATE AS fecha_expedicion_documento,
        TRY_STRPTIME(TRIM(FECHAOCURRENCIA), '%d/%m/%Y')::DATE AS fecha_ocurrencia,
        TRY_STRPTIME(TRIM(FEcharEPORTE), '%d/%m/%Y')::DATE AS fecha_reporte,
        TRY_STRPTIME(TRIM(FECHAVALORACION), '%d/%m/%Y')::DATE AS fecha_valoracion,
        
        -- Textos normalizados
        NULLIF(TRIM(LOWER(EXPEDICIONDOCUMENTO)), '') AS expedicion_documento,
        NULLIF(TRIM(LOWER(PERTENENCIAETNICA)), '') AS pertenencia_etnica,
        NULLIF(TRIM(UPPER(GENERO)), '') AS genero,
        NULLIF(TRIM(LOWER(TIPOHECHO)), '') AS tipo_hecho,
        NULLIF(TRIM(LOWER(HECHO)), '') AS hecho,
        
        -- Códigos convertidos a BIGINT
        TRY_CAST(TRIM(IDPERSONA) AS BIGINT) AS id_persona,
        TRY_CAST(TRIM(IDHOGAR) AS BIGINT) AS id_hogar,
        TRY_CAST(TRIM(DOCUMENTO) AS BIGINT) documento,
        TRY_CAST(NULLIF(TRIM(CODDANEMUNICIPIOOCURRENCIA), '') AS BIGINT) AS cod_dane_municipio_ocurrencia,
        TRY_CAST(NULLIF(TRIM(CODDANEMUNICIPIORESIDENCIA), '') AS BIGINT) AS cod_dane_municipio_residencia,
        TRY_CAST(NULLIF(TRIM(CODDANEDECLARACION), '') AS BIGINT) AS cod_dane_declaracion,
        TRY_CAST(NULLIF(TRIM(CODDANELLEGADA), '') AS BIGINT) AS cod_dane_llegada,
        TRY_CAST(NULLIF(TRIM(IDSINIESTRO), '') AS BIGINT) AS id_siniestro,
        TRY_CAST(NULLIF(TRIM(IDMIJEFE), '') AS BIGINT) AS id_mi_jefe,
        TRY_CAST(NULLIF(TRIM(CODIGOHECHO), '') AS BIGINT) AS codigo_hecho,
        TRY_CAST(NULLIF(TRIM(CONSPERSONA), '') AS BIGINT) AS cons_persona,
        TRY_CAST(NULLIF(TRIM(DISCAPACIDAD), '') AS BIGINT) AS discapacidad,
        TRY_CAST(NULLIF(TRIM(REGISTRADURIA), '') AS BIGINT) AS registraduria,
        
        -- Ubicaciones
        NULLIF(TRIM(LOWER(ZONAOCURRENCIA)), '') AS zona_ocurrencia,
        NULLIF(TRIM(LOWER(UBICACIONOCURRENCIA)), '') AS ubicacion_ocurrencia,
        NULLIF(TRIM(LOWER(ZONARESIDENCIA)), '') AS zona_residencia,
        NULLIF(TRIM(LOWER(UBICACIONRESIDENCIA)), '') AS ubicacion_residencia,
        NULLIF(TRIM(LOWER(DIRECCION)), '') AS direccion,
        NULLIF(TRIM(LOWER(PAIS)), '') AS pais,
        NULLIF(TRIM(LOWER(CIUDAD)), '') AS ciudad,
        
        -- Actores
        NULLIF(TRIM(LOWER(PRESUNTOACTOR)), '') AS presunto_actor,
        NULLIF(TRIM(LOWER(PRESUNTOVICTIMIZANTE)), '') AS presunto_victimizante,
        
        -- Tipos
        NULLIF(TRIM(LOWER(TIPOPOBLACION)), '') AS tipo_poblacion,
        NULLIF(TRIM(LOWER(TIPOVICTIMA)), '') AS tipo_victima,
        NULLIF(TRIM(LOWER(TIPODESPLAZAMIENTO)), '') AS tipo_desplazamiento,
        
        -- Contacto
        NULLIF(TRIM(NUMTELEFONOFIJO), '') AS num_telefono_fijo,
        NULLIF(TRIM(NUMTELEFONOCELULAR), '') AS num_telefono_celular,
        NULLIF(TRIM(LOWER(EMAIL)), '') AS email,
        
        -- Estados y otros
        NULLIF(TRIM(LOWER(ESTADOVICTIMA)), '') AS estado_victima,
        NULLIF(TRIM(VIGENCIADOCUMENTO), '') AS vigencia_documento,
        NULLIF(TRIM(LOWER(RELACION)), '') AS relacion,
        
        -- Discapacidad
        NULLIF(TRIM(LOWER(DESCRIPCIONDISCAPACIDAD)), '') AS descripcion_discapacidad,
        
        -- Otros
        NULLIF(TRIM(FUD_FICHA), '') AS fud_ficha,
        NULLIF(TRIM(LOWER(AFECTACIONES)), '') AS afectaciones
        
    FROM source_data
)

SELECT * FROM cleaned
