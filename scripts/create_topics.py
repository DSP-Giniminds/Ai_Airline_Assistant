import os
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv("/home/airline-ai/.env")

config = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
    "client.id": "airline-ai-topic-admin"
}

admin = AdminClient(config)

event_retention = str(7 * 24 * 60 * 60 * 1000)
history_retention = str(30 * 24 * 60 * 60 * 1000)

topic_configs = {
    "flight_status": {
        "cleanup.policy": "compact"
    },
    "flight_telemetry": {
        "cleanup.policy": "delete",
        "retention.ms": event_retention
    },
    "passenger_itineraries": {
        "cleanup.policy": "compact"
    },
    "booking_events": {
        "cleanup.policy": "delete",
        "retention.ms": history_retention
    },
    "airport_events": {
        "cleanup.policy": "delete",
        "retention.ms": event_retention
    },
    "hotel_inventory": {
        "cleanup.policy": "compact"
    },
    "passenger_state": {
        "cleanup.policy": "compact"
    },
    "rebooking_events": {
        "cleanup.policy": "delete",
        "retention.ms": history_retention
    }
}

topics = [
    NewTopic(
        name,
        num_partitions=3,
        replication_factor=3,
        config=configs
    )
    for name, configs in topic_configs.items()
]

print("\n========== AIRLINE KAFKA TOPIC CREATION ==========\n")

results = admin.create_topics(topics, request_timeout=30)

failed = False

for name, future in results.items():
    try:
        future.result()
        print(f"CREATED: {name}")
    except Exception as exc:
        if "TOPIC_ALREADY_EXISTS" in str(exc):
            print(f"ALREADY EXISTS: {name}")
        else:
            failed = True
            print(f"FAILED: {name} -> {exc}")

print("\n========== TOPIC VERIFICATION ==========\n")

metadata = admin.list_topics(timeout=30)

for name in sorted(topic_configs):
    if name in metadata.topics:
        topic = metadata.topics[name]
        if topic.error is None:
            print(f"VERIFIED: {name} | Partitions: {len(topic.partitions)}")
        else:
            print(f"ERROR: {name} | {topic.error}")
            failed = True
    else:
        print(f"MISSING: {name}")
        failed = True

if failed:
    raise SystemExit("Topic creation or verification failed.")

print("\nALL AIRLINE TOPICS VERIFIED SUCCESSFULLY")
