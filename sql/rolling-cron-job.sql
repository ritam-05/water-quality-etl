-- 1. Enable the pg_cron extension
create extension if not exists pg_cron;

-- 2. Create the cleanup function for your tables
create or replace function purge_old_water_data()
returns void as $$
begin
    -- Delete telemetry readings older than 5 days
    delete from water_quality_readings 
    where event_timestamp < now() - interval '5 days';
    
    -- Delete audit logs older than 5 days
    delete from pipeline_audit 
    where processed_at < now() - interval '5 days';
end;
$$ language plpgsql;

-- 3. Schedule the cron job to run daily at midnight UTC
select cron.schedule(
    'purge-5-day-data',
    '0 0 * * *',
    'select purge_old_water_data();'
);