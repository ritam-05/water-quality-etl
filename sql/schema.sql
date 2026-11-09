-- 1. Dimension Table: Stores static information about our IoT stations
CREATE TABLE IF NOT EXISTS dim_station (
    station_id VARCHAR(50) PRIMARY KEY,
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Audit Table: Tracks every batch processed by our ETL pipeline
CREATE TABLE IF NOT EXISTS pipeline_audit (
    batch_id UUID PRIMARY KEY,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    records_processed INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_message TEXT
);

-- 3. Fact Table: Stores abnormal readings for our Grafana Alerts dashboard
CREATE TABLE IF NOT EXISTS fact_alerts (
    alert_id UUID PRIMARY KEY,
    station_id VARCHAR(50) REFERENCES dim_station(station_id),
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    parameter_name VARCHAR(50) NOT NULL,
    alert_severity VARCHAR(20) NOT NULL,
    reading_value NUMERIC,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Fact Table: Stores the actual sensor readings
-- We keep this flat and numeric so Grafana can easily calculate averages
CREATE TABLE IF NOT EXISTS water_quality_readings (
    event_id UUID PRIMARY KEY,
    station_id VARCHAR(50) REFERENCES dim_station(station_id),
    event_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    ph NUMERIC,
    dissolved_oxygen NUMERIC,
    turbidity NUMERIC,
    temperature NUMERIC,
    specific_conductance NUMERIC,
    streamflow NUMERIC,
    status VARCHAR(20),
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Indexes: These dramatically speed up Grafana's time-series queries
CREATE INDEX IF NOT EXISTS idx_readings_timestamp ON water_quality_readings(event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_readings_station ON water_quality_readings(station_id);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON fact_alerts(event_timestamp DESC);