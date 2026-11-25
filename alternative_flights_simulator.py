import os
import time
import random
import signal
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

load_dotenv("/home/airline-ai/.env", override=True)

TOPIC = "alternative_flights"
IST = timezone(timedelta(hours=5, minutes=30))

airlines = [
    ("Air India", "AI"),
    ("IndiGo", "6E"),
    ("Akasa Air", "QP"),
    ("SpiceJet", "SG"),
    ("Air India Express", "IX")
]

# Prioritize BOM -> DEL alternatives
routes = [
    (("BOM", "DEL"), 60),
    (("BOM", "BLR"), 10),
    (("BOM", "HYD"), 10),
    (("DEL", "BOM"), 10),
    (("BLR", "DEL"), 10)
]

schema = """
{
  "type": "record",
  "name": "AlternativeFlight",
  "namespace": "airline.poc",
  "fields": [
    {"name": "flight_id", "type": "string"},
    {"name": "airline", "type": "string"},
    {"name": "origin", "type": "string"},
    {"name": "destination", "type": "string"},
    {"name": "departure_time", "type": "string"},
    {"name": "arrival_time", "type": "string"},
    {"name": "status", "type": "string"},
    {"name": "available_seats", "type": "int"},
    {"name": "price", "type": "double"},
    {"name": "currency", "type": "string"},
    {"name": "event_ts", "type": "long"}
  ]
}
"""

sr = SchemaRegistryClient({
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": (
        os.environ["SCHEMA_REGISTRY_API_KEY"] + ":" +
        os.environ["SCHEMA_REGISTRY_API_SECRET"]
    )
})

producer = SerializingProducer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": AvroSerializer(sr, schema, lambda obj, ctx: obj)
})

running = True

def stop_handler(signum, frame):
    global running
    running = False
    print("\nStopping flight simulator...")

signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)

def generate_flight():
    airline, prefix = random.choice(airlines)
    (origin, destination), _ = random.choices(
        routes, weights=[weight for _, weight in routes]
    )[0]

    flight_id = f"{prefix}{random.randint(100, 9999)}"

    now = datetime.now(IST)
    departure = now + timedelta(minutes=random.randint(30, 600))
    arrival = departure + timedelta(minutes=random.randint(90, 240))

    status = random.choices(
        ["SCHEDULED", "DELAYED"],
        weights=[85, 15]
    )[0]

    return {
        "flight_id": flight_id,
        "airline": airline,
        "origin": origin,
        "destination": destination,
        "departure_time": departure.isoformat(),
        "arrival_time": arrival.isoformat(),
        "status": status,
        "available_seats": random.randint(0, 35),
        "price": round(random.uniform(3500, 12000), 2),
        "currency": "INR",
        "event_ts": int(time.time() * 1000)
    }

print("=" * 60)
print("AIRLINEIQ - ALTERNATIVE FLIGHT SIMULATOR")
print("Topic: alternative_flights")
print("BOM -> DEL route priority: 60%")
print("Publishing interval: 3 seconds")
print("Press CTRL+C to stop")
print("=" * 60)

count = 0

try:
    while running:
        record = generate_flight()

        producer.produce(
            topic=TOPIC,
            key=record["flight_id"],
            value=record
        )

        remaining = producer.flush(10)

        if remaining:
            print("WARNING: Message delivery pending.")
        else:
            count += 1
            print(
                f"[{count}] {record['flight_id']} | "
                f"{record['origin']} -> {record['destination']} | "
                f"{record['status']} | Seats: "
                f"{record['available_seats']} | INR {record['price']}"
            )

        time.sleep(3)

finally:
    producer.flush(15)
    print(f"Simulator stopped. Total records: {count}")
