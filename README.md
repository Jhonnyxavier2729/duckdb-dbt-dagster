# Pipeline de Datos con Dagster y dbt

Pipeline de datos completo construido con **Dagster**, **dbt** y **DuckDB** para procesar, transformar y analizar datos.

## Descripción

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) que:

1. **Extrae** datos de archivos CSV con encoding especial
2. **Transforma** los datos usando dbt con múltiples capas de modelado
3. **Carga** los datos procesados a PostgreSQL y exporta a CSV

## Arquitectura

```
┌─────────────┐
│  Raw Data   │  (entrada.txt con delimitador »)
│  (CSV)      │
└──────┬──────┘
       │
       ↓
┌─────────────┐
│  DuckDB     │  Schema: raw
│  (Staging)  │
└──────┬──────┘
       │
       ↓
┌─────────────┐
│  dbt        │  Transformaciones:
│  Pipeline   │  - Staging
│             │  - Intermediate
│             │  - Refined
│             │  - Marts
└──────┬──────┘
       │
       ├──────────────┬────────────┐
       ↓              ↓            ↓
┌─────────────┐  ┌──────────┐  ┌──────────┐
│  PostgreSQL │  │   CSV    │  │  Reports │
│  (Prod)     │  │  Export  │  │  (Excel) │
└─────────────┘  └──────────┘  └──────────┘
```

## Estructura del Proyecto

```
pipeline-sql/
├── dagster_code/              # Orquestación con Dagster
│   ├── src/dagster_code/
│   │   └── defs/
│   │       ├── assets.py      # Definición de assets
│   │       ├── resources.py   # Recursos (DuckDB, dbt)
│   │       └── schedules.py   # Programación de tareas
│   ├── dbt_code/              # Transformaciones dbt
│   │   ├── models/
│   │   │   ├── staging/       # Capa de staging
│   │   │   ├── intermediate/  # Transformaciones intermedias
│   │   │   ├── refined/       # Datos refinados
│   │   │   └── marts/         # Modelos analíticos
│   │   └── dbt_project.yml
│   └── data/
│       ├── raw/               # Datos de entrada
│       ├── duckdb/           # Base de datos DuckDB
│       ├── output/           # Exports CSV
│       └── report/           # Reportes Excel
└── export_report_to_excel.py # Exportador de reportes
```

## Instalación y Configuración

### Prerrequisitos

- Python 3.9+
- PostgreSQL (opcional, para carga final)
- Poetry o pip

### 1. Instalar dependencias

```bash
cd dagster_code
pip install -e ".[dev]"
```

O con Poetry:
```bash
poetry install
```

### 2. Configurar variables de entorno

modifica el archivo `.env.example` en la raíz del proyecto:

```env
# PostgreSQL Credentials (opcional)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nombre_base_datos
POSTGRES_USER=usuario
POSTGRES_PASSWORD=password
```

### 3. Generar manifest de dbt

```bash
python dagster_code/generate_dbt_manifest.py
```

## Ejecución

### Modo Desarrollo (Dagster UI)

```bash
cd dagster_code
dagster dev
```

Acceder a la interfaz web en: `http://localhost:3000`

### Ejecutar el pipeline completo

Desde la interfaz de Dagster:
1. Ir a la pestaña **Assets**
2. Seleccionar todos los assets
3. Hacer clic en **Materialize all**

O desde la línea de comandos:
```bash
dagster asset materialize --select "*"
```

## Assets del Pipeline

### 1. `load_csv_to_duckdb`
- Lee el archivo CSV con encoding latin-1
- Delimitador especial: `»`
- Carga los datos en la tabla raw en DuckDB

### 2. `dbt_project_assets`
- Ejecuta todas las transformaciones dbt
- Capas: staging → intermediate → refined → marts
- Genera modelos analíticos segmentados

### 3. `export_to_csv`
- Exporta los datos limpios a CSV
- Delimitador: `|` (pipe)
- Ubicación: `data/output/victimas_cleaned.csv`

### 4. `load_to_postgres`
- Carga los datos finales a PostgreSQL
- Tabla: `public.ref_victimas_cleaned`
- Requiere configuración en `.env`

## Modelos dbt

### Staging
- `stg_victimas_start`: Datos iniciales del raw layer

### Intermediate
- `int_victimas_intermediate`: Limpieza y transformaciones intermedias

### Refined
- `ref_victimas_cleaned`: Datos limpios y validados

### Marts (Análisis)
- `mart_ciudad`: Agregaciones por ciudad
- `mart_discapacidad`: Análisis por tipo de discapacidad
- `mart_edad`: Distribución por edad
- `mart_etnia`: Análisis étnico
- `mart_genero`: Análisis por género

## Comandos Útiles

### dbt

```bash
cd dagster_code/dbt_code

# Ejecutar todos los modelos
dbt run

# Ejecutar tests
dbt test

# Generar documentación
dbt docs generate
dbt docs serve

# Ejecutar modelo específico
dbt run --select stg_victimas_start
```

### Dagster

```bash
# Ver listado de assets
dagster asset list

# Materializar asset específico
dagster asset materialize --select load_csv_to_duckdb

# Ver logs
dagster run list
```

## Testing

```bash
# Tests de dbt
dbt test

# Tests de Python
pytest tests/
```

## Exportar Reportes a Excel

```bash
python export_report_to_excel.py
```

Genera reportes Excel en `data/report/`

## Troubleshooting

### Error: "Missing PostgreSQL credentials"
- Verificar que el archivo `.env` existe y contiene todas las credenciales
- Las credenciales son opcionales si no se usa el asset `load_to_postgres`

### Error: "manifest.json not found"
- Ejecutar: `python dagster_code/generate_dbt_manifest.py`
- O: `cd dagster_code/dbt_code && dbt parse`

### Error de encoding en CSV
- El archivo debe tener encoding `latin-1`
- Verificar que el delimitador es `»` (ASCII 187)

## Contribuir

1. Crear una rama feature: `git checkout -b feature/nueva-funcionalidad`
2. Hacer commit de los cambios: `git commit -am 'Agregar nueva funcionalidad'`
3. Push a la rama: `git push origin feature/nueva-funcionalidad`
4. Crear un Pull Request

## Licencia

[Especificar licencia]

## Autores

[Especificar autores]

---

**Nota**: Este proyecto está en desarrollo activo. Consultar la documentación de cada componente para más detalles.
