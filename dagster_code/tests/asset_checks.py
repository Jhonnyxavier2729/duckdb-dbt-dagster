"""Asset Checks - Quality validations for Dagster assets"""

import dagster as dg
from dagster_duckdb import DuckDBResource
from pathlib import Path


# =====================================================
# Asset Checks para load_csv_to_duckdb
# =====================================================

@dg.asset_check(asset="load_csv_to_duckdb")
def check_raw_table_exists(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica que la tabla raw.victimas exista en DuckDB"""
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name = 'victimas'
        """).fetchone()[0]
        
        passed = result == 1
        return dg.AssetCheckResult(
            passed=passed,
            description="La tabla raw.victimas debe existir en la base de datos",
            metadata={
                "table_exists": dg.MetadataValue.bool(passed)
            }
        )


@dg.asset_check(asset="load_csv_to_duckdb")
def check_raw_table_not_empty(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica que la tabla raw.victimas contenga datos"""
    with duckdb.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM raw.victimas").fetchone()[0]
        
        passed = count > 0
        return dg.AssetCheckResult(
            passed=passed,
            description=f"La tabla debe contener al menos 1 registro. Encontrados: {count:,}",
            metadata={
                "record_count": dg.MetadataValue.int(count),
                "is_empty": dg.MetadataValue.bool(count == 0)
            }
        )


@dg.asset_check(asset="load_csv_to_duckdb", blocking=True)
def check_minimum_records(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica que la tabla tenga un número mínimo razonable de registros"""
    MIN_RECORDS = 100
    
    with duckdb.get_connection() as conn:
        count = conn.execute("SELECT COUNT(*) FROM raw.victimas").fetchone()[0]
        
        passed = count >= MIN_RECORDS
        return dg.AssetCheckResult(
            passed=passed,
            description=f"La tabla debe contener al menos {MIN_RECORDS:,} registros. Encontrados: {count:,}",
            metadata={
                "record_count": dg.MetadataValue.int(count),
                "minimum_required": dg.MetadataValue.int(MIN_RECORDS),
                "difference": dg.MetadataValue.int(count - MIN_RECORDS)
            }
        )


# @dg.asset_check(asset="load_csv_to_duckdb")
# def check_no_null_only_rows(duckdb: DuckDBResource) -> dg.AssetCheckResult:
#     """Verifica que no haya filas completamente nulas"""
#     with duckdb.get_connection() as conn:
#         # Obtener todas las columnas
#         columns = conn.execute("""
#             SELECT column_name 
#             FROM information_schema.columns 
#             WHERE table_schema = 'raw' AND table_name = 'victimas'
#         """).fetchall()
#         
#         column_names = [col[0] for col in columns]
#         
#         # Construir query para detectar filas donde todas las columnas son NULL
#         null_checks = " AND ".join([f'"{col}" IS NULL' for col in column_names])
#         query = f"SELECT COUNT(*) FROM raw.victimas WHERE {null_checks}"
#         
#         null_rows = conn.execute(query).fetchone()[0]
#         
#         passed = null_rows == 0
#         return dg.AssetCheckResult(
#             passed=passed,
#             description=f"No debe haber filas completamente nulas. Encontradas: {null_rows}",
#             metadata={
#                 "null_rows": dg.MetadataValue.int(null_rows),
#                 "total_columns": dg.MetadataValue.int(len(column_names))
#             }
#         )


@dg.asset_check(asset="load_csv_to_duckdb")
def check_expected_columns(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica que la tabla tenga columnas esperadas"""
    EXPECTED_MIN_COLUMNS = 10
    
    with duckdb.get_connection() as conn:
        count = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_schema = 'raw' AND table_name = 'victimas'
        """).fetchone()[0]
        
        passed = count >= EXPECTED_MIN_COLUMNS
        return dg.AssetCheckResult(
            passed=passed,
            description=f"La tabla debe tener al menos {EXPECTED_MIN_COLUMNS} columnas. Encontradas: {count}",
            metadata={
                "column_count": dg.MetadataValue.int(count),
                "minimum_expected": dg.MetadataValue.int(EXPECTED_MIN_COLUMNS)
            }
        )


@dg.asset_check(asset="load_csv_to_duckdb")
def check_raw_schema_exists(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica que el schema 'raw' exista"""
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.schemata 
            WHERE schema_name = 'raw'
        """).fetchone()[0]
        
        passed = result == 1
        return dg.AssetCheckResult(
            passed=passed,
            description="El schema 'raw' debe existir en la base de datos",
            metadata={
                "schema_exists": dg.MetadataValue.bool(passed)
            }
        )


# =====================================================
# Asset Checks para DBT marts
# =====================================================

@dg.asset_check(asset="dbt_project_assets")
def check_marts_schema_exists(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica que el schema 'marts' exista después de ejecutar DBT"""
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.schemata 
            WHERE schema_name = 'marts'
        """).fetchone()[0]
        
        passed = result == 1
        return dg.AssetCheckResult(
            passed=passed,
            description="El schema 'marts' debe existir después de ejecutar DBT",
            metadata={
                "schema_exists": dg.MetadataValue.bool(passed)
            }
        )


@dg.asset_check(asset="dbt_project_assets")
def check_marts_tables_exist(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica que las tablas marts principales existan"""
    EXPECTED_MARTS = ['mart_ciudad', 'mart_genero', 'mart_edad', 'mart_etnia', 'mart_discapacidad']
    
    with duckdb.get_connection() as conn:
        existing_tables = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'marts'
        """).fetchall()
        
        existing_table_names = [t[0] for t in existing_tables]
        missing_tables = [mart for mart in EXPECTED_MARTS if mart not in existing_table_names]
        
        passed = len(missing_tables) == 0
        return dg.AssetCheckResult(
            passed=passed,
            description=f"Todas las tablas marts deben existir. Faltantes: {missing_tables if missing_tables else 'ninguna'}",
            metadata={
                "expected_marts": dg.MetadataValue.int(len(EXPECTED_MARTS)),
                "existing_marts": dg.MetadataValue.int(len(existing_table_names)),
                "missing_marts": dg.MetadataValue.text(str(missing_tables)),
                "found_marts": dg.MetadataValue.text(str(existing_table_names))
            }
        )


@dg.asset_check(asset="dbt_project_assets")
def check_mart_tables_not_empty(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica que las tablas marts contengan datos"""
    with duckdb.get_connection() as conn:
        marts = conn.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'marts'
        """).fetchall()
        
        empty_marts = []
        mart_counts = {}
        
        for mart in marts:
            table_name = mart[0]
            count = conn.execute(f"SELECT COUNT(*) FROM marts.{table_name}").fetchone()[0]
            mart_counts[table_name] = count
            
            if count == 0:
                empty_marts.append(table_name)
        
        passed = len(empty_marts) == 0
        return dg.AssetCheckResult(
            passed=passed,
            description=f"Todas las tablas marts deben contener datos. Vacías: {empty_marts if empty_marts else 'ninguna'}",
            metadata={
                "empty_marts": dg.MetadataValue.text(str(empty_marts)),
                "mart_counts": dg.MetadataValue.md(
                    "\n".join([f"- **{name}**: {count:,} registros" for name, count in mart_counts.items()])
                )
            }
        )


@dg.asset_check(asset="dbt_project_assets")
def check_refined_table_exists(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica que la tabla refined.ref_victimas_cleaned exista"""
    with duckdb.get_connection() as conn:
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'refined' AND table_name = 'ref_victimas_cleaned'
        """).fetchone()[0]
        
        passed = result == 1
        return dg.AssetCheckResult(
            passed=passed,
            description="La tabla refined.ref_victimas_cleaned debe existir",
            metadata={
                "table_exists": dg.MetadataValue.bool(passed)
            }
        )


@dg.asset_check(asset="dbt_project_assets", blocking=True)
def check_data_quality_metrics(duckdb: DuckDBResource) -> dg.AssetCheckResult:
    """Verifica métricas de calidad de datos en la tabla refined"""
    with duckdb.get_connection() as conn:
        # Verificar que la tabla refined exista primero
        table_exists = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'refined' AND table_name = 'ref_victimas_cleaned'
        """).fetchone()[0]
        
        if table_exists == 0:
            return dg.AssetCheckResult(
                passed=False,
                description="La tabla refined.ref_victimas_cleaned no existe",
            )
        
        # Obtener total de registros
        total_records = conn.execute("SELECT COUNT(*) FROM refined.ref_victimas_cleaned").fetchone()[0]
        
        # Verificar que tenga datos
        passed = total_records > 0
        
        return dg.AssetCheckResult(
            passed=passed,
            description=f"La tabla refined debe contener datos. Registros: {total_records:,}",
            metadata={
                "total_records": dg.MetadataValue.int(total_records),
                "data_exists": dg.MetadataValue.bool(passed)
            }
        )


# =====================================================
# Asset Checks para reportes
# =====================================================

@dg.asset_check(asset="generate_excel_report")
def check_report_file_created(context: dg.AssetCheckExecutionContext) -> dg.AssetCheckResult:
    """Verifica que el archivo de reporte Excel fue creado"""
    BASE_DIR = Path(__file__).parent.parent
    REPORT_DIR = BASE_DIR / "data" / "report"
    
    # Buscar el archivo más reciente en el directorio
    if REPORT_DIR.exists():
        report_files = list(REPORT_DIR.glob("reporte_victimas_*.xlsx"))
        
        if report_files:
            latest_report = max(report_files, key=lambda p: p.stat().st_mtime)
            file_size = latest_report.stat().st_size
            
            # Verificar que el archivo no esté vacío
            passed = file_size > 1000  # Al menos 1KB
            
            return dg.AssetCheckResult(
                passed=passed,
                description=f"El reporte Excel fue creado. Tamaño: {file_size:,} bytes",
                metadata={
                    "file_path": dg.MetadataValue.path(str(latest_report)),
                    "file_size_bytes": dg.MetadataValue.int(file_size),
                    "file_size_kb": dg.MetadataValue.float(round(file_size / 1024, 2)),
                    "report_count": dg.MetadataValue.int(len(report_files))
                }
            )
    
    return dg.AssetCheckResult(
        passed=False,
        description="No se encontró ningún archivo de reporte Excel",
        metadata={
            "report_dir": dg.MetadataValue.path(str(REPORT_DIR))
        }
    )


@dg.asset_check(asset="generate_excel_report")
def check_report_directory_exists() -> dg.AssetCheckResult:
    """Verifica que el directorio de reportes exista"""
    BASE_DIR = Path(__file__).parent.parent
    REPORT_DIR = BASE_DIR / "data" / "report"
    
    passed = REPORT_DIR.exists() and REPORT_DIR.is_dir()
    
    return dg.AssetCheckResult(
        passed=passed,
        description=f"El directorio de reportes {'existe' if passed else 'no existe'}",
        metadata={
            "report_dir": dg.MetadataValue.path(str(REPORT_DIR)),
            "exists": dg.MetadataValue.bool(passed)
        }
    )
