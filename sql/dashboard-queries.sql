-- ============================================================================
-- Water Quality Grafana Dashboard Queries
-- ============================================================================
-- Run each query in a Grafana PostgreSQL panel using the Supabase data source.
-- Grafana expands $__timeFilter(event_timestamp) using the dashboard time range.

-- ============================================================================
-- Dashboard 1: Operational Monitoring & Alerts
-- ============================================================================

-- 1. Critical Alerts (Anomalies)
-- Table: show only readings that exceed water-quality safety thresholds.
SELECT
  event_timestamp AS "Time",
  station_id AS "Station ID",
  ph AS "pH Level",
  turbidity AS "Turbidity"
FROM water_quality_readings
WHERE ph < 6.5 OR ph > 8.5 OR turbidity > 15
ORDER BY event_timestamp DESC
LIMIT 10;

-- 2. Latest Current Readings
-- Stat or gauge cards: show the newest payload; COALESCE displays zero for a
-- missing measurement so an occasional dropped sensor field does not break UI.
SELECT
  COALESCE(ph, 0) AS "Current pH",
  COALESCE(turbidity, 0) AS "Current Turbidity",
  COALESCE(temperature, 0) AS "Current Temp"
FROM water_quality_readings
ORDER BY event_timestamp DESC
LIMIT 1;

-- 3. Average Metrics by Station
-- Bar chart: compare baseline pH and turbidity across monitoring locations.
SELECT
  station_id,
  AVG(ph) AS "Average pH",
  AVG(turbidity) AS "Average Turbidity"
FROM water_quality_readings
GROUP BY station_id;

-- 4. Streamflow & Conductance
-- Filled area chart: compare water volume with dissolved-particle concentration.
SELECT
  event_timestamp AS "time",
  streamflow AS "Streamflow (cfs)",
  specific_conductance AS "Conductance"
FROM water_quality_readings
WHERE $__timeFilter(event_timestamp)
ORDER BY event_timestamp ASC;

-- 5. Temperature & Dissolved Oxygen
-- Time-series line chart: establish the baseline conditions for aquatic life.
SELECT
  event_timestamp AS "time",
  temperature AS "Temperature",
  dissolved_oxygen AS "Dissolved Oxygen"
FROM water_quality_readings
WHERE $__timeFilter(event_timestamp)
ORDER BY event_timestamp ASC;

-- ============================================================================
-- Dashboard 2: Advanced Trend Analysis & Diagnostics
-- ============================================================================

-- 6. pH Volatility Profile
-- Bar gauge: compare minimum, average, and maximum pH to reveal instability.
SELECT
  station_id,
  MIN(ph) AS "Min pH",
  AVG(ph) AS "Avg pH",
  MAX(ph) AS "Max pH"
FROM water_quality_readings
GROUP BY station_id;

-- 7. Hourly Contaminant Averages
-- Time-series chart: aggregate five-minute telemetry into hourly time buckets.
SELECT
  DATE_TRUNC('hour', event_timestamp) AS "time",
  AVG(specific_conductance) AS "Avg Conductance"
FROM water_quality_readings
WHERE $__timeFilter(event_timestamp)
GROUP BY 1
ORDER BY 1 ASC;

-- 8. Suffocation Correlation
-- Dual-axis time-series chart: contrast temperature with dissolved oxygen.
SELECT
  event_timestamp AS "time",
  temperature AS "Temperature (°C)",
  dissolved_oxygen AS "Dissolved Oxygen (mg/L)"
FROM water_quality_readings
WHERE $__timeFilter(event_timestamp)
ORDER BY event_timestamp ASC;

-- 9. The Sediment Kick-Up
-- Overlay chart: determine whether water movement correlates with cloudiness.
SELECT
  event_timestamp AS "time",
  streamflow AS "Streamflow (cfs)",
  turbidity AS "Turbidity (NTU)"
FROM water_quality_readings
WHERE $__timeFilter(event_timestamp)
ORDER BY event_timestamp ASC;

-- 10. The Flash Flood Indicator
-- Time-series bar chart: LAG calculates the streamflow delta between readings.
SELECT
  event_timestamp AS "time",
  streamflow - LAG(streamflow, 1) OVER (ORDER BY event_timestamp ASC)
    AS "Flow Surge/Drop (Delta)"
FROM water_quality_readings
WHERE $__timeFilter(event_timestamp)
ORDER BY event_timestamp ASC;
