import json
import boto3
import uuid
from datetime import datetime

s3 = boto3.client('s3')

BUCKET_NAME = 'ecommerce-event-data-goutham'  # Replace if different
FOLDER_NAME = 'raw-events/'  # Folder where events are stored

def ensure_folder_exists():
    try:
        # Creates a placeholder object to ensure folder exists in S3
        s3.put_object(Bucket=BUCKET_NAME, Key=FOLDER_NAME + '.keep', Body='')
        print(f"✅ Ensured folder '{FOLDER_NAME}' exists in S3")
    except Exception as e:
        print(f"⚠️ Could not ensure folder: {e}")

def lambda_handler(event, context):
    try:
        ensure_folder_exists()

        # Support both direct invocation and API Gateway POST
        body = json.loads(event["body"]) if "body" in event else event

        # Validate expected keys
        required_keys = ['user_id', 'action', 'product_id', 'price']
        if not all(k in body for k in required_keys):
            return {
                'statusCode': 400,
                'body': json.dumps('⚠️ Invalid or incomplete event. Expected keys: user_id, action, product_id, price')
            }

        # Add event_time
        body['event_time'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

        # Generate unique S3 key
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
        unique_id = str(uuid.uuid4())
        key = f"{FOLDER_NAME}{timestamp}_{unique_id}.json"

        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=json.dumps(body),
            ContentType='application/json'
        )

        print(f"✅ Event stored in S3: {key}")
        return {
            'statusCode': 200,
            'body': json.dumps('✅ Event with timestamp stored in S3')
        }

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"❌ Error: {str(e)}")
        }
