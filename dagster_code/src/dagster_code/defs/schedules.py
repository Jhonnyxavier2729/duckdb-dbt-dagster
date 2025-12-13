import dagster as dg

# Schedule que ejecuta el pipeline completo cada 5 minutos
pipeline_schedule = dg.ScheduleDefinition(
    name="pipeline_schedule_5min",
    cron_schedule="*/5 * * * *",  # Cada 5 minutos
    target="*",  # Ejecuta todos los assets
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
