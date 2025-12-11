from kafka import KafkaProducer
from kafka import KafkaConsumer

import json

alert_producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],    # Indirizzo broker nel Docker
    client_id='AlertSystem-Producer',    # <--- Un ID unico
    batch_size=16384,
    linger_ms=50,
    max_in_flight_requests_per_connection=5,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

alert_consumer = KafkaConsumer(
    'to-alert-system',
    bootstrap_servers=['kafka:9092'],
    client_id='AlertSystem-Consumer',
    group_id='alert-system-group',
    max_poll_records=500,
    fetch_max_wait_ms=500,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    enable_auto_commit=False
)