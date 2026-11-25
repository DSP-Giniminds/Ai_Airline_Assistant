
import os
import json
import time
import signal
import random

from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv("/home/airline-ai/.env", override=True)

IST = timezone(timedelta(hours=5, minutes=30))

STATUS_INTERVAL = 15
ALTERNATIVE_REFRESH_INTERVAL = 300

# full = publish all flight and passenger events
# alternatives_only = publish alternative flights without
# overwriting existing passenger itinerary records.
SIMULATOR_MODE = os.getenv(
    "SIMULATOR_MODE", "full"
).strip().lower()

RUN_ID = datetime.now(IST).strftime("%Y%m%dT%H%M%S")

running = True

required_env = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET"
]

missing = [key for key in required_env if not os.getenv(key)]

if missing:
    raise RuntimeError(
        "Missing environment variables: " + ", ".join(missing)
    )


# ============================================================
# KAFKA CONFIGURATION
# ============================================================

KAFKA_CONFIG = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
    "client.id": "airline-ai-unified-simulator",
    "acks": "all",
    "enable.idempotence": True
}


# ============================================================
# SCHEMA REGISTRY
# ============================================================

SR = SchemaRegistryClient({
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": (
        os.environ["SCHEMA_REGISTRY_API_KEY"] + ":" +
        os.environ["SCHEMA_REGISTRY_API_SECRET"]
    )
})


# ============================================================
# AVRO SCHEMAS
# Existing Schema Registry structures are preserved.
# ============================================================

FLIGHT_STATUS_SCHEMA = {
    "type": "record",
    "name": "FlightStatus",
    "namespace": "com.airline.events",
    "fields": [
        {"name": "flight_id", "type": "string"},
        {"name": "airline", "type": "string"},
        {"name": "origin", "type": "string"},
        {"name": "destination", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "delay_minutes", "type": "int"},
        {"name": "scheduled_arrival", "type": "string"},
        {"name": "estimated_arrival", "type": "string"},
        {"name": "gate", "type": "string"},
        {"name": "event_ts", "type": "long"},
        {"name": "delay_reason", "type": ["null", "string"], "default": None}
    ]
}


PASSENGER_SCHEMA = {
    "type": "record",
    "name": "PassengerItinerary",
    "namespace": "com.airline.events",
    "fields": [
        {"name": "passenger_id", "type": "string"},
        {"name": "booking_id", "type": "string"},
        {"name": "flight_id", "type": "string"},
        {"name": "next_flight_id", "type": "string"},
        {"name": "connection_airport", "type": "string"},
        {"name": "scheduled_connection_minutes", "type": "int"},
        {"name": "event_ts", "type": "long"}
    ]
}


ALTERNATIVE_SCHEMA = {
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


# ============================================================
# PRODUCER FACTORY
# ============================================================

def create_producer(schema):

    serializer = AvroSerializer(
        SR,
        json.dumps(schema),
        to_dict=lambda obj, ctx: obj
    )

    return SerializingProducer({
        **KAFKA_CONFIG,
        "key.serializer": StringSerializer("utf_8"),
        "value.serializer": serializer
    })


producers = {
    "flight_status": create_producer(FLIGHT_STATUS_SCHEMA),
    "flight_status_v2": create_producer(FLIGHT_STATUS_SCHEMA),
    "passenger_itineraries": create_producer(PASSENGER_SCHEMA),
    "alternative_flights": create_producer(ALTERNATIVE_SCHEMA)
}


# ============================================================
# COMMON FUNCTIONS
# ============================================================

def event_time():
    return int(time.time() * 1000)


def get_delay_reason(delay):
    if delay <= 0:
        return None

    reasons = {
        10: "Late departure from previous airport",
        15: "Late arrival of incoming aircraft",
        25: "Airport operational congestion",
        35: "Air traffic congestion",
        45: "Technical inspection before departure",
        55: "Adverse weather conditions"
    }

    return reasons.get(delay, "Operational delay")


def publish(topic, key, record):

    producers[topic].produce(
        topic=topic,
        key=key,
        value=record
    )

    producers[topic].poll(0)


def flush_topic(topic):

    remaining = producers[topic].flush(15)

    if remaining:
        raise RuntimeError(
            f"{topic}: {remaining} messages were not delivered"
        )


def flush_all():

    for topic in producers:
        flush_topic(topic)


def stop_handler(signum, frame):

    global running

    running = False

    print("\nStopping Airline AI simulator gracefully...")


signal.signal(signal.SIGINT, stop_handler)
signal.signal(signal.SIGTERM, stop_handler)


# ============================================================
# FLIGHT SCENARIOS
#
# Unique IDs avoid overwriting existing flight records.
# AI417 remains untouched.
# ============================================================

scenario_start = datetime.now(IST)

FLIGHTS = [
    {
        "flight_id": f"AI581-{RUN_ID}",
        "airline": "Air India",
        "origin": "BLR",
        "destination": "DEL",
        "gate": "B12",
        "scheduled_arrival": scenario_start + timedelta(hours=8)
    },
    {
        "flight_id": f"6E217-{RUN_ID}",
        "airline": "IndiGo",
        "origin": "BOM",
        "destination": "DEL",
        "gate": "C04",
        "scheduled_arrival": scenario_start + timedelta(hours=9)
    }
]


# ============================================================
# PASSENGER ITINERARIES
#
# New passengers only.
# Existing P1001-P1005 are untouched.
# ============================================================

PASSENGERS = [
    {
        "passenger_id": "P2001",
        "booking_id": "BK2001",
        "flight_index": 0,
        "next_flight_id": "AI602",
        "connection_minutes": 35
    },
    {
        "passenger_id": "P2002",
        "booking_id": "BK2002",
        "flight_index": 0,
        "next_flight_id": "6E604",
        "connection_minutes": 50
    },
    {
        "passenger_id": "P2003",
        "booking_id": "BK2003",
        "flight_index": 0,
        "next_flight_id": "UK803",
        "connection_minutes": 75
    },
    {
        "passenger_id": "P2004",
        "booking_id": "BK2004",
        "flight_index": 1,
        "next_flight_id": "AI602",
        "connection_minutes": 40
    },
    {
        "passenger_id": "P2005",
        "booking_id": "BK2005",
        "flight_index": 1,
        "next_flight_id": "6E604",
        "connection_minutes": 60
    },
    {
        "passenger_id": "P2006",
        "booking_id": "BK2006",
        "flight_index": 1,
        "next_flight_id": "UK803",
        "connection_minutes": 85
    }
]


# ============================================================
# INITIAL FLIGHT STATUS
# ============================================================

def publish_initial_flights():

    print("\n========== INITIAL FLIGHT STATUS & INVENTORY ==========")

    for flight in FLIGHTS:

        now = datetime.now(IST)
        scheduled = flight["scheduled_arrival"]

        departure = now + timedelta(minutes=random.randint(15, 45))
        arrival = departure + timedelta(hours=random.randint(2, 3))

        status_record = {
            "flight_id": flight["flight_id"],
            "airline": flight["airline"],
            "origin": flight["origin"],
            "destination": flight["destination"],
            "status": "ON_TIME",
            "delay_minutes": 0,
            "scheduled_arrival": scheduled.isoformat(),
            "estimated_arrival": scheduled.isoformat(),
            "gate": flight["gate"],
            "event_ts": event_time(),
            "delay_reason": None
        }

        publish("flight_status", flight["flight_id"], status_record)
        publish("flight_status_v2", flight["flight_id"], status_record)

        available_seats = random.randint(18, 36)
        price = round(random.uniform(5000, 10000), 2)

        inventory_record = {
            "flight_id": flight["flight_id"],
            "airline": flight["airline"],
            "origin": flight["origin"],
            "destination": flight["destination"],
            "departure_time": departure.isoformat(),
            "arrival_time": arrival.isoformat(),
            "status": "SCHEDULED",
            "available_seats": available_seats,
            "price": price,
            "currency": "INR",
            "event_ts": event_time()
        }

        publish(
            "alternative_flights",
            flight["flight_id"],
            inventory_record
        )

        print(
            f"FLIGHT | {flight['flight_id']} | "
            f"{flight['origin']} -> {flight['destination']} | ON_TIME"
        )

        print(
            f"INVENTORY | {flight['flight_id']} | "
            f"Departure: {departure.strftime('%H:%M')} | "
            f"Seats: {available_seats} | INR {price}"
        )

    flush_topic("flight_status")
    flush_topic("flight_status_v2")
    flush_topic("alternative_flights")


# PASSENGER ITINERARIES
# ============================================================

def publish_passengers():

    print("\n========== PASSENGER ITINERARIES ==========")

    for passenger in PASSENGERS:

        flight = FLIGHTS[passenger["flight_index"]]

        record = {
            "passenger_id": passenger["passenger_id"],
            "booking_id": passenger["booking_id"],
            "flight_id": flight["flight_id"],
            "next_flight_id": passenger["next_flight_id"],
            "connection_airport": "DEL",
            "scheduled_connection_minutes":
                passenger["connection_minutes"],
            "event_ts": event_time()
        }

        publish(
            "passenger_itineraries",
            passenger["passenger_id"],
            record
        )

        print(
            f"PASSENGER | {passenger['passenger_id']} | "
            f"{flight['flight_id']} | "
            f"Connection: {passenger['connection_minutes']} min"
        )

    flush_topic("passenger_itineraries")


# ============================================================
# ALTERNATIVE FLIGHT GENERATION
#
# New unique flight IDs for every refresh.
# Topic remains append-only.
# ============================================================

alternative_batch = 0



def publish_alternative_flights():

    global alternative_batch
    alternative_batch += 1

    now = datetime.now(IST)

    print("\n========== ALTERNATIVE DEPARTURES ==========")

    templates = [
        {"prefix": "AI621", "airline": "Air India",
         "departure_offset": 35, "seats": 12, "price": 6500},
        {"prefix": "6E204", "airline": "IndiGo",
         "departure_offset": 75, "seats": 18, "price": 5800},
        {"prefix": "QP143", "airline": "Akasa Air",
         "departure_offset": 120, "seats": 9, "price": 7200}
    ]

    for origin, destination in [("BLR", "DEL"), ("BOM", "DEL")]:
        for template in templates:

            departure = now + timedelta(
                minutes=template["departure_offset"]
            )
            arrival = departure + timedelta(hours=2, minutes=30)

            flight_id = (
                f"{template['prefix']}-{origin}-"
                f"{RUN_ID}-B{alternative_batch:04d}"
            )

            available_seats = max(
                0, template["seats"] - random.randint(0, 5)
            )
            price = round(
                template["price"] + random.uniform(-300, 700), 2
            )

            record = {
                "flight_id": flight_id,
                "airline": template["airline"],
                "origin": origin,
                "destination": destination,
                "departure_time": departure.isoformat(),
                "arrival_time": arrival.isoformat(),
                "status": "SCHEDULED",
                "available_seats": available_seats,
                "price": price,
                "currency": "INR",
                "event_ts": event_time()
            }

            publish("alternative_flights", flight_id, record)

            print(
                f"ALTERNATIVE | {flight_id} | "
                f"{origin} -> {destination} | "
                f"Departure: {departure.strftime('%H:%M')} | "
                f"Seats: {available_seats} | INR {price}"
            )

    flush_topic("alternative_flights")


# ============================================================
# CONTINUOUS FLIGHT STATUS UPDATES
# ============================================================

DELAY_PATTERN = [
    0,
    15,
    35,
    55,
    45,
    25,
    10,
    0
]


def publish_flight_updates(cycle):

    print("\n========== LIVE FLIGHT UPDATES ==========")

    for index, flight in enumerate(FLIGHTS):

        delay = DELAY_PATTERN[
            (cycle + index * 2) % len(DELAY_PATTERN)
        ]

        scheduled = flight["scheduled_arrival"]

        estimated = scheduled + timedelta(minutes=delay)

        status = "DELAYED" if delay > 0 else "ON_TIME"

        record = {
            "flight_id": flight["flight_id"],
            "airline": flight["airline"],
            "origin": flight["origin"],
            "destination": flight["destination"],
            "status": status,
            "delay_minutes": delay,
            "scheduled_arrival": scheduled.isoformat(),
            "estimated_arrival": estimated.isoformat(),
            "gate": flight["gate"],
            "event_ts": event_time(),
            "delay_reason": get_delay_reason(delay)
        }

        # Preserve existing Flink topic.
        publish(
            "flight_status",
            flight["flight_id"],
            record
        )

        # Publish the same event to the MCP-compatible topic.
        publish(
            "flight_status_v2",
            flight["flight_id"],
            record
        )

        print(
            f"UPDATE | {flight['flight_id']} | "
            f"{flight['origin']} -> {flight['destination']} | "
            f"{status} | Delay: {delay} min"
        )

    flush_topic("flight_status")
    flush_topic("flight_status_v2")


# ============================================================
# START SIMULATOR
# ============================================================

print("=" * 65)
print("AIRLINE AI - UNIFIED FLIGHT INGESTION SIMULATOR")
print("=" * 65)

print("Run ID:", RUN_ID)
print("Flight status topic: flight_status")
print("Passenger topic: passenger_itineraries")
print("Alternative topic: alternative_flights")
print("Status update interval:", STATUS_INTERVAL, "seconds")
print(
    "Alternative refresh interval:",
    ALTERNATIVE_REFRESH_INTERVAL,
    "seconds"
)

print("\nRoutes:")
print("BLR -> DEL")
print("BOM -> DEL")

print("\nPress CTRL+C to stop.")
print("=" * 65)


try:

    if SIMULATOR_MODE == "full":
        # Normal full simulator startup.
        publish_initial_flights()
        publish_passengers()

    elif SIMULATOR_MODE == "alternatives_only":
        print(
            "SAFE MODE: Skipping initial flight and "
            "passenger itinerary publishing."
        )

    else:
        raise RuntimeError(
            f"Invalid SIMULATOR_MODE: {SIMULATOR_MODE}"
        )

    # Publish alternatives immediately on startup.
    publish_alternative_flights()

    cycle = 0

    last_alternative_refresh = time.monotonic()

    # Continuous execution
    while running:

        cycle += 1

        if SIMULATOR_MODE == "full":
            publish_flight_updates(cycle)

        current_time = time.monotonic()

        if (
            current_time - last_alternative_refresh
            >= ALTERNATIVE_REFRESH_INTERVAL
        ):

            publish_alternative_flights()

            last_alternative_refresh = current_time

        print(
            f"\nCycle {cycle} completed. "
            f"Next flight update in {STATUS_INTERVAL} seconds."
        )

        for _ in range(STATUS_INTERVAL):

            if not running:
                break

            time.sleep(1)


except Exception as exc:

    print("\nSIMULATOR ERROR:", str(exc))

    raise

finally:

    print("\n========== FLUSHING KAFKA PRODUCERS ==========")

    flush_all()

    print("All Kafka producers flushed.")

    print("AIRLINE AI SIMULATOR STOPPED.")


