{# {{ config(materialized='table') }}

SELECT *
FROM read_csv_auto(
    '../Notebook/input/entrada.txt',
    delim='»',
    header=True,
    encoding='latin-1',
    ignore_errors=True,
    all_varchar=True
) #}
