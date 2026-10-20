from kafka import KafkaProducer, KafkaConsumer
import json

BOOTSTRAP_SERVERS = 'localhost:29092'

EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]

consumer = KafkaConsumer(
    'raw_events',
    bootstrap_servers= BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='silver_stream_processor',
    key_deserializer=lambda k: k.decode('utf-8') if k else None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def is_valid_event(event):
    if not event.get('customer_id'):
        return False
    if event.get('event_type') not in EVENT_TYPES:
        return False
    if not isinstance(event.get('amount'), (int, float)) or event.get("amount") is None or event.get('amount') <= 0:
        return False
    if not event.get('currency'):
        return False
    return True 

print("Starting Stream Processor...")

for message in consumer:
    key = message.key
    event = message.value

    if is_valid_event(event):
        producer.send(topic='clean_events', key=key, value=event)
       
        print(f"FORWARDED | key={key} | event_type={event['event_type']}")

    else:
        print(f"DROPPED | key={key} | reason=invalid")

    consumer.commit()