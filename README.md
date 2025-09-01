# AWS

1. S3 Bucket

Purpose: Centralized storage for raw data, processed data (Delta tables), libraries, checkpoints, and scripts.
Folders:

libs/: Stores JARs or Python wheels for Glue (e.g., Delta Lake).
source/customer-data/: Contains customer CSV files (e.g., customers_20250831.csv).
source/booking-data/: Contains booking CSV files (e.g., bookings_20250831.csv), triggering the workflow.
checkpoint/: Holds processed_files.json to track processed dates.
scripts/: Stores the Glue script (BookingETLJob.py).
delta-table/: Stores processed Delta tables (booking_fact, customer_scd).

2. Lambda Function

Role: Acts as an orchestrator, triggered by S3 PUT events.
Input: S3 event with s3_object_key.
Action: Calls the Glue job with the event data.
IAM Role: LambdaRole with permissions for S3, Glue, and SNS.

3. Glue Job

Role: Performs ETL on booking and customer data.
Process:

Reads CSV files from source/ folders.
Applies transformations (e.g., aggregations, joins) and writes to Delta tables.
Updates checkpoint/processed_files.json.
Emits custom events (e.g., via boto3.client('events').put_events) for failures or anomalies.

IAM Role: GlueRole with S3, Delta, and SNS access.

4. EventBridge

Role: Event router for Glue job state changes and custom events.
Rules:

Glue Job State Change: Triggers on FAILED state, routes to SNS.
Custom Events: Captures data quality or checkpoint anomalies, routes to SNS.

IAM Role: Default EventBridge role with Lambda and SNS permissions.

5. SNS

Role: Notification service for subscribers.
Topic: GlueNotifications with subscriptions (e.g., email, SMS).
Action: Sends alerts based on EventBridge events.
