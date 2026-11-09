import json
import uuid
import random
import os
from datetime import datetime, timezone, timedelta
import boto3

# Load environment variables for local testing
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

def generate_sensor_data(num_stations=25, mins_per_batch=5):
    regions = ["North", "South", "East", "West", "Central"]
    records = []
    
    # Start time is 5 minutes ago to simulate the batch collection period
    base_time = datetime.now(timezone.utc) - timedelta(minutes=mins_per_batch)
    
    for _ in range(mins_per_batch):
        base_time += timedelta(minutes=1)
        for i in range(1, num_stations + 1):
            station_id = f"USGS-STAT-{i:03d}"
            
            # Baseline realistic values
            ph = round(random.uniform(6.5, 8.5), 2)
            do = round(random.uniform(6.0, 10.0), 2)
            turbidity = round(random.uniform(1.0, 50.0), 2)
            temp = round(random.uniform(5.0, 25.0), 2)
            cond = round(random.uniform(50.0, 1500.0), 2)
            flow = round(random.uniform(10.0, 5000.0), 2)
            
            status = "OK"
            
            # Inject Anomalies (~5% chance) to test our ETL later
            if random.random() < 0.05:
                status = "ANOMALY"
                ph = round(random.uniform(1.0, 14.0), 2) # Extreme pH
                
            # Inject Missing Data (~2% chance) simulating sensor failure
            if random.random() < 0.02:
                turbidity = None
                status = "MISSING_DATA"
                
            record = {
                "event_id": str(uuid.uuid4()),
                "station_id": station_id,
                "region": regions[i % 5],
                "event_timestamp": base_time.isoformat(),
                "ingestion_timestamp": datetime.now(timezone.utc).isoformat(),
                "status": status,
                "measurements": {
                    "pH": {"value": ph, "unit": "pH"},
                    "dissolved_oxygen": {"value": do, "unit": "mg/L"},
                    "turbidity": {"value": turbidity, "unit": "FNU"},
                    "temperature": {"value": temp, "unit": "C"},
                    "specific_conductance": {"value": cond, "unit": "uS/cm"},
                    "streamflow": {"value": flow, "unit": "cfs"}
                }
            }
            records.append(record)
    return records

def lambda_handler(event, context):
    """AWS Lambda entry point"""
    queue_url = os.environ.get('SQS_QUEUE_URL')
    if not queue_url:
        raise ValueError("SQS_QUEUE_URL environment variable is missing")
        
    # Boto3 automatically uses the IAM user we configured locally
    sqs = boto3.client('sqs', region_name=os.environ.get('AWS_REGION', 'us-east-1'))
    
    print("Generating IoT sensor batch...")
    records = generate_sensor_data(num_stations=25, mins_per_batch=5)
    
    # Package all 125 records into a single JSON payload
    payload = json.dumps(records)
    
    print(f"Sending {len(records)} records to SQS...")
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=payload
    )
    
    print(f"Success! MessageId: {response['MessageId']}")
    return {
        'statusCode': 200,
        'body': f"Batch sent. MessageId: {response['MessageId']}"
    }

# This allows us to run the file directly on our laptop for testing
if __name__ == "__main__":
    lambda_handler(None, None)