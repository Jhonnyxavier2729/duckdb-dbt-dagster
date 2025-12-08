-- Reference to raw data using dbt source
-- Configured as view since it will be under constant load
-- Normalize column names: lowercase and snake_case

WITH source AS (
    
    SELECT * FROM {{ source('raw', 'victimas') }}

),

renamed_and_casted AS (

    SELECT
        -- Text columns (keep as VARCHAR)
        ORIGEN AS origen,
        FUENTE AS fuente,
        PROGRAMA AS programa,
        TIPODOCUMENTO AS tipo_documento,
        PRIMERNOMBRE AS primer_nombre,
        SEGUNDONOMBRE AS segundo_nombre,
        PRIMERAPELLIDO AS primer_apellido,
        SEGUNDOAPELLIDO AS segundo_apellido,
        NOMBRECOMPLETO AS nombre_completo,
        EXPEDICIONDOCUMENTO AS expedicion_documento,
        PERTENENCIAETNICA AS pertenencia_etnica,
        GENERO AS genero,
        TIPOHECHO AS tipo_hecho,
        HECHO AS hecho,
        ZONAOCURRENCIA AS zona_ocurrencia,
        UBICACIONOCURRENCIA AS ubicacion_ocurrencia,
        ZONARESIDENCIA AS zona_residencia,
        UBICACIONRESIDENCIA AS ubicacion_residencia,
        DIRECCION AS direccion,
        PRESUNTOACTOR AS presunto_actor,
        PRESUNTOVICTIMIZANTE AS presunto_victimizante,
        TIPOPOBLACION AS tipo_poblacion,
        TIPOVICTIMA AS tipo_victima,
        TIPODESPLAZAMIENTO AS tipo_desplazamiento,
        NUMTELEFONOFIJO AS num_telefono_fijo,
        NUMTELEFONOCELULAR AS num_telefono_celular,
        PAIS AS pais,
        CIUDAD AS ciudad,
        EMAIL AS email,
        ESTADOVICTIMA AS estado_victima,
        VIGENCIADOCUMENTO AS vigencia_documento,
        RELACION AS relacion,
        DESCRIPCIONDISCAPACIDAD AS descripcion_discapacidad,
        FUD_FICHA AS fud_ficha,
        AFECTACIONES AS afectaciones,
        
        -- Date columns (cast from VARCHAR DD/MM/YYYY to DATE)
        TRY_STRPTIME(FECHANACIMIENTO, '%d/%m/%Y')::DATE AS fecha_nacimiento,
        TRY_STRPTIME(FECHAEXPEDICIONDOCUMENTO, '%d/%m/%Y')::DATE AS fecha_expedicion_documento,
        TRY_STRPTIME(FECHAOCURRENCIA, '%d/%m/%Y')::DATE AS fecha_ocurrencia,
        TRY_STRPTIME(FEcharEPORTE, '%d/%m/%Y')::DATE AS fecha_reporte,
        TRY_STRPTIME(FECHAVALORACION, '%d/%m/%Y')::DATE AS fecha_valoracion,
        
        -- ID columns (cast to BIGINT)
        TRY_CAST(IDPERSONA AS BIGINT) AS id_persona,
        TRY_CAST(IDHOGAR AS BIGINT) AS id_hogar,
        TRY_CAST(DOCUMENTO AS BIGINT) AS documento,
        TRY_CAST(CODDANEMUNICIPIOOCURRENCIA AS BIGINT) AS cod_dane_municipio_ocurrencia,
        TRY_CAST(CODDANEMUNICIPIORESIDENCIA AS BIGINT) AS cod_dane_municipio_residencia,
        TRY_CAST(CODDANEDECLARACION AS BIGINT) AS cod_dane_declaracion,
        TRY_CAST(CODDANELLEGADA AS BIGINT) AS cod_dane_llegada,
        TRY_CAST(IDSINIESTRO AS BIGINT) AS id_siniestro,
        TRY_CAST(IDMIJEFE AS BIGINT) AS id_mi_jefe,
        TRY_CAST(CODIGOHECHO AS BIGINT) AS codigo_hecho,
        TRY_CAST(CONSPERSONA AS BIGINT) AS cons_persona,
        TRY_CAST(DISCAPACIDAD AS BIGINT) AS discapacidad,
        TRY_CAST(REGISTRADURIA AS BIGINT) AS registraduria
        
    FROM source

)

SELECT * FROM renamed_and_casted

