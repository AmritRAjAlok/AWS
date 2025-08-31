import sys
import json
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sum as _sum, count, countDistinct, trim, to_date
from delta.tables import DeltaTable

# Initialize Spark and Glue context
sc = SparkContext.getOrCreate()
sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore") \
    .getOrCreate()
glueContext = GlueContext(sc)
job = Job(glueContext)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_object_key'])
job.init(args['JOB_NAME'], args)

# Initialize AWS clients
s3_client = boto3.client('s3')
events = boto3.client('events')

# Extract date from s3_object_key
s3_object_key = args.get('s3_object_key', '')
date_str = s3_object_key.split('_')[-1].replace('.csv', '')
print(f"Debug: Processing date_str={date_str}")

# Define S3 paths
bucket_name = "deproject911"
booking_key = f"source/booking-data/bookings_{date_str}.csv"
customer_key = f"source/customer-data/customers_{date_str}.csv"
checkpoint_key = "checkpoints/processed_files.json"
booking_data = f"s3://{bucket_name}/{booking_key}"
customer_data = f"s3://{bucket_name}/{customer_key}"
fact_table_path = f"s3://{bucket_name}/delta-table/booking_fact"
scd_table_path = f"s3://{bucket_name}/delta-table/customer_scd"

# Check if both files exist
def file_exists(bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except s3_client.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise

if not (file_exists(bucket_name, booking_key) and file_exists(bucket_name, customer_key)):
    events.put_events(Entries=[{
        'Source': 'custom.glue',
        'DetailType': 'Glue File Check Failure',
        'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': f'Missing paired file for {date_str}'}),
        'EventBusName': 'default'
    }])
    raise ValueError(f"Missing paired file for {date_str}")

# CHECKPOINT FETCH
try:
    response = s3_client.get_object(Bucket=bucket_name, Key=checkpoint_key)
    checkpoint_data = json.loads(response['Body'].read().decode('utf-8'))
except s3_client.exceptions.ClientError:
    checkpoint_data = {"processed_dates": []}  # Initialize if checkpoint file doesn't exist
print(f"Debug: Checkpoint fetched: {checkpoint_data}")
if date_str in checkpoint_data.get('processed_dates', []):
    events.put_events(Entries=[{
        'Source': 'custom.glue',
        'DetailType': 'Glue Checkpoint Failure',
        'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': f'File pair for {date_str} already processed'}),
        'EventBusName': 'default'
    }])
    raise ValueError(f"File pair for {date_str} already processed. To reprocess, remove {date_str} from s3://{bucket_name}/{checkpoint_key}")

# Read booking data
booking_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("quote", "\"") \
    .option("multiLine", "true") \
    .load(booking_data)
booking_count = booking_df.count()
booking_df.printSchema()
booking_df.show(5)

# Read customer data
customer_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("quote", "\"") \
    .option("multiLine", "true") \
    .load(customer_data)
customer_df = customer_df.withColumn("valid_from", to_date(col("valid_from"), "yyyy-MM-dd")) \
                        .withColumn("valid_to", to_date(col("valid_to"), "yyyy-MM-dd"))
customer_count = customer_df.count()
customer_df.printSchema()
customer_df.show(5)

# Data Quality Checks
if booking_count == 0:
    events.put_events(Entries=[{
        'Source': 'custom.glue',
        'DetailType': 'Glue Data Quality Failure',
        'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': 'Booking Data Check Failed: Size is 0'}),
        'EventBusName': 'default'
    }])
    raise ValueError("Booking Data Check Failed: Size is 0")

booking_id_unique = booking_df.select(countDistinct("booking_id").alias("distinct_count"), count("booking_id").alias("total_count")).collect()[0]
if booking_id_unique.distinct_count != booking_id_unique.total_count:
    events.put_events(Entries=[{
        'Source': 'custom.glue',
        'DetailType': 'Glue Data Quality Failure',
        'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': 'Booking Data Check Failed: booking_id is not unique'}),
        'EventBusName': 'default'
    }])
    raise ValueError("Booking Data Check Failed: booking_id is not unique")

required_booking_columns = ["customer_id", "booking_date", "amount", "booking_type", "booking_status"]
for column in required_booking_columns:
    null_count = booking_df.filter(col(column).isNull()).count()
    if null_count > 0:
        events.put_events(Entries=[{
            'Source': 'custom.glue',
            'DetailType': 'Glue Data Quality Failure',
            'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': f'Booking Data Check Failed: {column} has null values'}),
            'EventBusName': 'default'
        }])
        raise ValueError(f"Booking Data Check Failed: {column} has null values")

for column in ["amount", "quantity", "discount"]:
    negative_count = booking_df.filter(col(column) < 0).count()
    if negative_count > 0:
        events.put_events(Entries=[{
            'Source': 'custom.glue',
            'DetailType': 'Glue Data Quality Failure',
            'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': f'Booking Data Check Failed: {column} has negative values'}),
            'EventBusName': 'default'
        }])
        raise ValueError(f"Booking Data Check Failed: {column} has negative values")

hotel_bookings = booking_df.filter(col("booking_type") == "Hotel")
hotel_total_rows = hotel_bookings.count()
if hotel_total_rows > 0:
    hotel_non_null_rows = hotel_bookings.filter((col("hotel_name").isNotNull()) & (trim(col("hotel_name")) != "")).count()
    hotel_completeness = hotel_non_null_rows / hotel_total_rows
    print(f"Debug: hotel_name completeness = {hotel_non_null_rows}/{hotel_total_rows} = {hotel_completeness:.2%}")
    hotel_bookings.select("booking_id", "booking_type", "hotel_name").show(truncate=False)
    if hotel_completeness < 0.5:
        events.put_events(Entries=[{
            'Source': 'custom.glue',
            'DetailType': 'Glue Data Quality Failure',
            'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': f'Booking Data Check Failed: hotel_name completeness is {hotel_completeness:.2%}'}),
            'EventBusName': 'default'
        }])
        raise ValueError(f"Booking Data Check Failed: hotel_name completeness is {hotel_completeness:.2%}, expected >= 50% for Hotel bookings")

flight_bookings = booking_df.filter(col("booking_type") == "Flight")
flight_total_rows = flight_bookings.count()
if flight_total_rows > 0:
    flight_non_null_rows = flight_bookings.filter((col("flight_number").isNotNull()) & (trim(col("flight_number")) != "")).count()
    flight_completeness = flight_non_null_rows / flight_total_rows
    print(f"Debug: flight_number completeness = {flight_non_null_rows}/{flight_total_rows} = {flight_completeness:.2%}")
    flight_bookings.select("booking_id", "booking_type", "flight_number").show(truncate=False)
    if flight_completeness < 0.8:
        events.put_events(Entries=[{
            'Source': 'custom.glue',
            'DetailType': 'Glue Data Quality Failure',
            'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': f'Booking Data Check Failed: flight_number completeness is {flight_completeness:.2%}'}),
            'EventBusName': 'default'
        }])
        raise ValueError(f"Booking Data Check Failed: flight_number completeness is {flight_completeness:.2%}, expected >= 80% for Flight bookings")

if customer_count == 0:
    events.put_events(Entries=[{
        'Source': 'custom.glue',
        'DetailType': 'Glue Data Quality Failure',
        'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': 'Customer Data Check Failed: Size is 0'}),
        'EventBusName': 'default'
    }])
    raise ValueError("Customer Data Check Failed: Size is 0")

customer_id_unique = customer_df.select(countDistinct("customer_id").alias("distinct_count"), count("customer_id").alias("total_count")).collect()[0]
if customer_id_unique.distinct_count != customer_id_unique.total_count:
    events.put_events(Entries=[{
        'Source': 'custom.glue',
        'DetailType': 'Glue Data Quality Failure',
        'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': 'Customer Data Check Failed: customer_id is not unique'}),
        'EventBusName': 'default'
    }])
    raise ValueError("Customer Data Check Failed: customer_id is not unique")

required_customer_columns = ["customer_name", "customer_address", "email", "valid_from", "valid_to"]
for column in required_customer_columns:
    null_count = customer_df.filter(col(column).isNull()).count()
    if null_count > 0:
        events.put_events(Entries=[{
            'Source': 'custom.glue',
            'DetailType': 'Glue Data Quality Failure',
            'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': f'Customer Data Check Failed: {column} has null values'}),
            'EventBusName': 'default'
        }])
        raise ValueError(f"Customer Data Check Failed: {column} has null values")

# Transformations
booking_df_incremental = booking_df.withColumn("ingestion_time", current_timestamp()) \
    .withColumn("date_str", lit(date_str))
df_joined = booking_df_incremental.alias("b").join(customer_df.alias("c"), col("b.customer_id") == col("c.customer_id"), how='inner') \
    .select(
        col("b.booking_id"),
        col("b.customer_id"),
        col("b.booking_date"),
        col("b.amount"),
        col("b.booking_type"),
        col("b.quantity"),
        col("b.discount"),
        col("b.booking_status"),
        col("b.hotel_name"),
        col("b.flight_number"),
        col("b.ingestion_time"),
        col("b.date_str"),
        col("c.customer_name"),
        col("c.customer_address"),
        col("c.phone_number"),
        col("c.email"),
        col("c.valid_from"),
        col("c.valid_to")
    )
df_joined.show(10)

df_transform_agg = df_joined.withColumn("total_cost", col("amount") - col("discount")) \
    .filter(col("quantity") > 0) \
    .groupBy("booking_type", "customer_id", "date_str") \
    .agg(
        _sum("total_cost").cast("decimal(18,2)").alias("total_amount_sum"),
        _sum("quantity").cast("bigint").alias("total_quantity_sum")
    )
df_transform_agg.show()

# Write to Delta tables with schema alignment
fact_table_exists = DeltaTable.isDeltaTable(spark, fact_table_path)
if fact_table_exists:
    delta_table = DeltaTable.forPath(spark, fact_table_path)
    # Use mergeSchema to handle schema evolution if needed
    delta_table.alias("target") \
        .merge(
            source=df_transform_agg.alias("source"),
            condition="target.booking_type = source.booking_type AND target.customer_id = source.customer_id AND target.date_str = source.date_str"
        ) \
        .whenMatchedUpdate(set={
            "total_amount_sum": "source.total_amount_sum",
            "total_quantity_sum": "source.total_quantity_sum"
        }) \
        .whenNotMatchedInsertAll() \
        .execute()
else:
    df_transform_agg.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .save(fact_table_path)

# SCD Type 2 Merge
scd_table_exists = DeltaTable.isDeltaTable(spark, scd_table_path)
if scd_table_exists:
    row_count_before = spark.read.format('delta').load(scd_table_path).count()
    active_count_before = spark.read.format('delta').load(scd_table_path).filter(col('valid_to') == '9999-12-31').count()
    print(f"Debug: customer_scd row count before merge: {row_count_before}")
    print(f"Debug: Active records (valid_to='9999-12-31'): {active_count_before}")
    scd_table = DeltaTable.forPath(spark, scd_table_path)
    customer_df_new = customer_df.select(
        col("customer_id"),
        col("customer_name"),
        col("customer_address"),
        col("phone_number"),
        col("email"),
        col("valid_from"),
        lit("9999-12-31").cast("date").alias("valid_to")
    )
    try:
        scd_table.alias("scd") \
            .merge(
                source=customer_df_new.alias("updates"),
                condition="scd.customer_id = updates.customer_id AND scd.valid_to = '9999-12-31'"
            ) \
            .whenMatchedUpdate(
                condition="""
                    scd.customer_name != updates.customer_name OR
                    scd.customer_address != updates.customer_address OR
                    scd.phone_number != updates.phone_number OR
                    scd.email != updates.email OR
                    scd.valid_from != updates.valid_from
                """,
                set={"valid_to": "updates.valid_from"}
            ) \
            .whenNotMatchedInsert(
                values={
                    "customer_id": "updates.customer_id",
                    "customer_name": "updates.customer_name",
                    "customer_address": "updates.customer_address",
                    "phone_number": "updates.phone_number",
                    "email": "updates.email",
                    "valid_from": "updates.valid_from",
                    "valid_to": "updates.valid_to"
                }
            ) \
            .execute()
    except Exception as e:
        events.put_events(Entries=[{
            'Source': 'custom.glue',
            'DetailType': 'Glue SCD Merge Failure',
            'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': str(e)}),
            'EventBusName': 'default'
        }])
        raise
    row_count_after = spark.read.format('delta').load(scd_table_path).count()
    active_count_after = spark.read.format('delta').load(scd_table_path).filter(col('valid_to') == '9999-12-31').count()
    print(f"Debug: customer_scd row count after merge: {row_count_after}")
    print(f"Debug: Active records (valid_to='9999-12-31') after merge: {active_count_after}")
    if row_count_after > row_count_before + customer_count:
        events.put_events(Entries=[{
            'Source': 'custom.glue',
            'DetailType': 'Glue Row Count Anomaly',
            'Detail': json.dumps({
                'job_name': args['JOB_NAME'],
                'date_str': date_str,
                'row_count_before': row_count_before,
                'row_count_after': row_count_after,
                'expected_max': row_count_before + customer_count
            }),
            'EventBusName': 'default'
        }])
else:
    print(f"Debug: Writing {customer_count} records to new customer_scd table")
    customer_df.write.format("delta").mode("overwrite").save(scd_table_path)

# CHECKPOINT STORE
checkpoint_data['processed_dates'] = checkpoint_data.get('processed_dates', []) + [date_str]
print(f"Debug: Writing checkpoint with processed_dates: {checkpoint_data['processed_dates']}")
s3_client.put_object(
    Bucket=bucket_name,
    Key=checkpoint_key,
    Body=json.dumps(checkpoint_data, indent=2)
)
response = s3_client.get_object(Bucket=bucket_name, Key=checkpoint_key)
updated_checkpoint = json.loads(response['Body'].read().decode('utf-8'))
print(f"Debug: Verified checkpoint after write: {updated_checkpoint}")
if date_str not in updated_checkpoint.get('processed_dates', []):
    events.put_events(Entries=[{
        'Source': 'custom.glue',
        'DetailType': 'Glue Checkpoint Failure',
        'Detail': json.dumps({'job_name': args['JOB_NAME'], 'date_str': date_str, 'error': 'Failed to update checkpoint'}),
        'EventBusName': 'default'
    }])
    raise ValueError(f"Failed to update checkpoint with {date_str}")

job.commit()