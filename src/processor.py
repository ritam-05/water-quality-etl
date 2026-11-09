import json
import os
import gzip
import urllib.request
import urllib.parse
import ssl
from datetime import datetime, timezone
import pg8000.dbapi

def lambda_handler(event, context):
    supa_url = os.environ['SUPABASE_URL']
    supa_key = os.environ['SUPABASE_SERVICE_KEY']
    db_uri = os.environ['SUPABASE_DB_URI']

    # Safely parse the exact Supabase Connection String
    parsed = urllib.parse.urlparse(db_uri)
    db_user = urllib.parse.unquote(parsed.username)
    db_password = urllib.parse.unquote(parsed.password)
    db_host = parsed.hostname
    db_port = parsed.port
    db_name = parsed.path.lstrip('/')

    # Force SSL for the pooler
    # Encrypt connection but bypass strict CA chain validation
    # Bypass strict CA chain validation for AWS Lambda
    ssl_ctx = ssl.create_default_context()
    ssl_ctx.check_hostname = False
    ssl_ctx.verify_mode = ssl.CERT_NONE
    
    conn = pg8000.dbapi.connect(
        user=db_user, 
        password=db_password, 
        database=db_name, 
        host=db_host, 
        port=db_port, 
        ssl_context=ssl_ctx
    )
    cursor = conn.cursor()

    for record in event['Records']:
        message_id = record['messageId']
        payload = record['body']
        
        try:
            # Idempotency Check
            cursor.execute("SELECT 1 FROM pipeline_audit WHERE batch_id = %s", (message_id,))
            if cursor.fetchone():
                continue

            # Upload Raw Data Backup
            compressed_data = gzip.compress(payload.encode('utf-8'))
            file_name = f"batch_{message_id}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.json.gz"
            
            upload_req = urllib.request.Request(
                f"{supa_url}/storage/v1/object/raw-telemetry/{file_name}",
                data=compressed_data,
                headers={
                    "Authorization": f"Bearer {supa_key}",
                    "Content-Type": "application/gzip"
                },
                method="POST"
            )
            urllib.request.urlopen(upload_req)

            # Process Data
            data = json.loads(payload)
            processed_count = 0
            
            for item in data:
                station_id = item['station_id']
                event_ts = item['event_timestamp']
                
                cursor.execute(
                    "INSERT INTO dim_station (station_id, region) VALUES (%s, %s) ON CONFLICT (station_id) DO NOTHING",
                    (station_id, item['region'])
                )
                
                m = item['measurements']
                ph = m['pH']['value']
                status = item['status']
                
                cursor.execute("""
                    INSERT INTO water_quality_readings 
                    (event_id, station_id, event_timestamp, ph, dissolved_oxygen, turbidity, temperature, specific_conductance, streamflow, status) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (event_id) DO NOTHING
                """, (
                    item['event_id'], station_id, event_ts, ph, m['dissolved_oxygen']['value'], 
                    m['turbidity']['value'], m['temperature']['value'], m['specific_conductance']['value'], 
                    m['streamflow']['value'], status
                ))
                processed_count += 1
                
            cursor.execute("INSERT INTO pipeline_audit (batch_id, records_processed, status) VALUES (%s, %s, %s) ON CONFLICT (batch_id) DO NOTHING", 
                           (message_id, processed_count, 'SUCCESS'))
            conn.commit()

        except Exception as e:
            conn.rollback()
            raise e 

    cursor.close()
    conn.close()
    return {"statusCode": 200, "body": "ETL Processed Successfully"}
