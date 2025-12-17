import dagster as dg

# Schedule that runs the complete pipeline every 5 minutes
pipeline_schedule = dg.ScheduleDefinition(
    name="pipeline_schedule_5min",
    cron_schedule="*/5 * * * *",  # Every 5 minutes
    target="*",  # Run all assets
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
