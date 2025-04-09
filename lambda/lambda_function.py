import json
import boto3
import uuid
from datetime import datetime

s3 = boto3.client('s3')
BUCKET_NAME = 'ecommerce-event-data-goutham'
FOLDER_NAME = 'raw-events/'

def ensure_folder_exists():
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=FOLDER_NAME + '.keep', Body='')
        print(f"✅ Ensured folder '{FOLDER_NAME}' exists in S3")
    except Exception as e:
        print(f"⚠️ Could not ensure folder: {e}")

def lambda_handler(event, context):
    # Ensure folder (prefix) exists in S3
    ensure_folder_exists()

    # Add timestamp
    event_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    event['event_time'] = event_time

    # Generate unique file name
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
    unique_id = str(uuid.uuid4())
    key = f"{FOLDER_NAME}{timestamp}_{unique_id}.json"

    # Upload event
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(event),
            ContentType='application/json'
        )
        print(f"✅ Event stored as {key}")
        return {
            'statusCode': 200,
            'body': json.dumps('✅ Event with timestamp stored in S3')
        }
    except Exception as e:
        print(f"❌ Failed to store event: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('❌ Failed to store event')
        }
