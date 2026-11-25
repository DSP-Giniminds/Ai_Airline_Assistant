import os
import json
import time
from dotenv import load_dotenv

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

load_dotenv("/home/airline-ai/.env")

KAFKA = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
}

SR = SchemaRegistryClient({
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": (
        os.environ["SCHEMA_REGISTRY_API_KEY"] + ":" +
        os.environ["SCHEMA_REGISTRY_API_SECRET"]
    )
})

def schema(name, fields):
    return {
        "type": "record",
        "name": name,
        "namespace": "com.airline.events",
        "fields": fields
    }

def field(name, field_type):
    return {"name": name, "type": field_type}

def event_time():
    return int(time.time() * 1000)

schemas = {
    "flight_status": schema("FlightStatus", [
        field("flight_id", "string"),
        field("airline", "string"),
        field("origin", "string"),
        field("destination", "string"),
        field("status", "string"),
        field("delay_minutes", "int"),
        field("scheduled_arrival", "string"),
        field("estimated_arrival", "string"),
        field("gate", "string"),
        field("event_ts", "long")
    ]),

    "flight_telemetry": schema("FlightTelemetry", [
        field("flight_id", "string"),
        field("latitude", "double"),
        field("longitude", "double"),
        field("speed_kmh", "double"),
        field("altitude_ft", "int"),
        field("event_ts", "long")
    ]),

    "passenger_itineraries": schema("PassengerItinerary", [
        field("passenger_id", "string"),
        field("booking_id", "string"),
        field("flight_id", "string"),
        field("next_flight_id", "string"),
        field("connection_airport", "string"),
        field("scheduled_connection_minutes", "int"),
        field("event_ts", "long")
    ]),

    "booking_events": schema("BookingEvent", [
        field("booking_id", "string"),
        field("passenger_id", "string"),
        field("flight_id", "string"),
        field("event_type", "string"),
        field("event_ts", "long")
    ]),

    "airport_events": schema("AirportEvent", [
        field("airport_code", "string"),
        field("flight_id", "string"),
        field("event_type", "string"),
        field("gate", "string"),
        field("message", "string"),
        field("event_ts", "long")
    ]),

    "hotel_inventory": schema("HotelInventory", [
        field("hotel_id", "string"),
        field("hotel_name", "string"),
        field("city", "string"),
        field("available_rooms", "int"),
        field("price_per_night", "double"),
        field("currency", "string"),
        field("event_ts", "long")
    ])
}

producers = {}

for topic, schema_obj in schemas.items():
    serializer = AvroSerializer(
        SR,
        json.dumps(schema_obj),
        to_dict=lambda obj, ctx: obj
    )

    producers[topic] = SerializingProducer({
        **KAFKA,
        "client.id": "airline-ai-producer",
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": serializer
    })

def publish(topic, key, value):
    producer = producers[topic]

    producer.produce(
        topic=topic,
        key=key,
        value=value
    )

    producer.poll(0)

    print(f"PUBLISHED | {topic} | {key}")

def flush_all():
    for topic, producer in producers.items():
        remaining = producer.flush(30)
        if remaining:
            raise RuntimeError(
                f"{topic}: {remaining} messages not delivered"
            )

print("\n========== AIRLINE EVENT SIMULATOR ==========\n")

now = event_time()

# 1. Initial flight status
publish("flight_status", "AI417", {
    "flight_id": "AI417",
    "airline": "Air India",
    "origin": "BOM",
    "destination": "DEL",
    "status": "ON_TIME",
    "delay_minutes": 0,
    "scheduled_arrival": "2026-11-18T14:30:00+05:30",
    "estimated_arrival": "2026-11-18T14:30:00+05:30",
    "gate": "A12",
    "event_ts": now
})

# 2. Flight telemetry
publish("flight_telemetry", "AI417", {
    "flight_id": "AI417",
    "latitude": 19.0760,
    "longitude": 72.8777,
    "speed_kmh": 820.5,
    "altitude_ft": 35000,
    "event_ts": now
})

# 3. Passenger itineraries
passengers = [
    ("P1001", "BK1001"),
    ("P1002", "BK1002"),
    ("P1003", "BK1003"),
    ("P1004", "BK1004"),
    ("P1005", "BK1005")
]

for passenger_id, booking_id in passengers:
    publish("passenger_itineraries", passenger_id, {
        "passenger_id": passenger_id,
        "booking_id": booking_id,
        "flight_id": "AI417",
        "next_flight_id": "AI602",
        "connection_airport": "DEL",
        "scheduled_connection_minutes": 55,
        "event_ts": now
    })

    publish("booking_events", booking_id, {
        "booking_id": booking_id,
        "passenger_id": passenger_id,
        "flight_id": "AI417",
        "event_type": "BOOKING_CONFIRMED",
        "event_ts": now
    })

# 4. Airport event
publish("airport_events", "DEL-AI417", {
    "airport_code": "DEL",
    "flight_id": "AI417",
    "event_type": "GATE_ASSIGNED",
    "gate": "A12",
    "message": "Flight gate assigned",
    "event_ts": now
})

# 5. Hotel inventory
publish("hotel_inventory", "HTL-DEL-001", {
    "hotel_id": "HTL-DEL-001",
    "hotel_name": "Airport Business Hotel",
    "city": "Delhi",
    "available_rooms": 12,
    "price_per_night": 4500.0,
    "currency": "INR",
    "event_ts": now
})

flush_all()

print("\n========== SIMULATING FLIGHT DISRUPTION ==========\n")
print("Waiting 10 seconds before delay update...")
time.sleep(10)

delay_time = event_time()

publish("flight_status", "AI417", {
    "flight_id": "AI417",
    "airline": "Air India",
    "origin": "BOM",
    "destination": "DEL",
    "status": "DELAYED",
    "delay_minutes": 45,
    "scheduled_arrival": "2026-11-18T14:30:00+05:30",
    "estimated_arrival": "2026-11-18T15:15:00+05:30",
    "gate": "A12",
    "event_ts": delay_time
})

publish("airport_events", "DEL-AI417", {
    "airport_code": "DEL",
    "flight_id": "AI417",
    "event_type": "FLIGHT_DELAYED",
    "gate": "A12",
    "message": "Flight delayed by 45 minutes",
    "event_ts": delay_time
})

flush_all()

print("\n========== SIMULATION COMPLETED ==========")
print("Source events successfully submitted to Kafka.")
