from kafka import KafkaProducer
import json
import random
import time
import uuid
from datetime import datetime


BOOTSTRAP_SERVERS = 'localhost:29092'

Topic_name = 'raw_events'

producer = KafkaProducer(
                    bootstrap_servers=BOOTSTRAP_SERVERS,
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8')
                )

EVENT_TYPES = ["PAGE_VIEW", "ADD_TO_CART", "PURCHASE"]
INVALID_EVENT_TYPES = ["CLICK", "VIEW", "PAY"]

def generate_event():
    is_valid = random.random() < 0.25

    customer_id = f'CUS_{random.randint(1, 1000)}'
    event_type = random.choice(EVENT_TYPES) 
    amount = round(random.uniform(10.0, 5000.0), 2)
    currency = "INR"

    Invalid_field = None

    if is_valid:
        Invalid_field = random.choice(['customer_id', 'event_type', 'amount', 'currency'])
    
    event = {
        "event_id": str(uuid.uuid4()),
        "customer_id": None if Invalid_field == 'customer_id' else customer_id,
        "event_type": random.choice(INVALID_EVENT_TYPES) if Invalid_field == 'event_type' else event_type,
        "amount": random.uniform(-500, -10) if Invalid_field == 'amount' else amount,
        "currency": None if Invalid_field == 'currency' else currency   ,
        "event_timestamp": datetime.utcnow().isoformat()
    }

    return event["customer_id"], event

print("Starting Kafka Producer...")

while True:
    key, value = generate_event()

    producer.send(topic = Topic_name, key=key, value=value)

    print(f"Produced event | key={key} | value={value}")

    time.sleep(1)