import json
import boto3
s3_client = boto3.client("s3")
glue_client = boto3.client('glue')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    s3_object_key = event.get('Records', [{}])[0].get('s3', {}).get('object', {}).get('key', 'source/customer-data/customers_2024-07-25.csv')
    date_str = s3_object_key.split('_')[-1].replace('.csv', '')
    print(f"Extracted date_str: {date_str}")

    booking_key = f'source/booking-data/bookings_{date_str}.csv'
    customer_key = f'source/customer-data/customers_{date_str}.csv'
    bucket_name = 'deproject911'

    def file_exists(bucket, key):
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except s3_client.exceptions.ClientError:
            return False

    if file_exists(bucket_name, booking_key) and file_exists(bucket_name, customer_key):
        print(f"Both files exist, starting job with s3_object_key={customer_key}")
        glue_client.start_job_run(
            JobName='BookingETLJob',
            Arguments={'--s3_object_key': customer_key}
        )
        return {'statusCode': 200, 'body': json.dumps(f'Job triggered for {date_str}')}
    else:
        print(f"Missing paired file for {date_str}")
        return {'statusCode': 404, 'body': json.dumps(f'Missing paired file for {date_str}')}