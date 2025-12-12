from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'to-notifier',
    bootstrap_servers=['kafka:9092'],
    client_id='Notifier-Consumer',
    group_id='notifier-group',
    max_poll_records=500,
    fetch_max_wait_ms=500,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    # 1010101010101010 => deserializzazione => Ciao
    enable_auto_commit=False
)