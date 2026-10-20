from kafka import KafkaConsumer
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import os
import pandas as pd


BOOTSTRAP_SERVERS = 'localhost:29092'

WAREHOUSE = os.getenv('COMPUTE_WH', 'COMPUTE_WH')  # fallback to default warehouse name

SNOWFLAKE_CONFIG = {
    "user" : os.environ["SNOWFLAKE_USER"],
    "password" : os.environ["SNOWFLAKE_PASSWORD"],
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "warehouse" : WAREHOUSE,
    "database" : os.getenv("SNOWFLAKE_DATABASE", "KAFKA_DB"),
    "schema" : os.getenv("SNOWFLAKE_SCHEMA", "KAFKA_DB_STREAMING")
}

BATCH_SIZE = 10

consumer = KafkaConsumer(
    'clean_events',
    bootstrap_servers= BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='gold_stream_processor',
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')) if v else None
)

sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)

print("Connected to Snowflake")
print("Starting Kafka -> Snowflake Loader...")


buffer = []

def flush_to_snowflake(records):
    df = pd.DataFrame(records)
    df.columns = [c.upper() for c in df.columns]

    success, nchunks, nrows, _ = write_pandas(
        conn=sf_conn,
        df=df,
        table_name="KAFKA_EVENTS_SILVER"
    )

    if not success:
        raise Exception("Snowflake insert failed")
    
    print(f"Inserted {nrows} rows into Snowflake")


for message in consumer:
    event = message.value
    buffer.append({
        "event_id": event["event_id"],
        "customer_id" : event["customer_id"],
        "event_type" : event["event_type"],
        "amount" : event["amount"],
        "currency" : event["currency"],
        "event_timestamp" : event["event_timestamp"]
    })

    if len(buffer) >= BATCH_SIZE:
        try:
            flush_to_snowflake(buffer)
            consumer.commit()
            buffer.clear()
        except Exception as e:
            print(f"ERROR inserting batch: {e}")