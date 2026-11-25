
import os
import json
import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer


# Load existing Confluent credentials
load_dotenv("/home/airline-ai/.env")

TOPIC = "hotel_inventory"

required_env = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
]

missing = [name for name in required_env if not os.getenv(name)]
if missing:
    raise RuntimeError(f"Missing environment variables: {missing}")


# Hotel inventory Avro value schema
schema_str = json.dumps({
    "type": "record",
    "name": "HotelInventory",
    "namespace": "airline.events",
    "fields": [
        {"name": "hotel_id", "type": "string"},
        {"name": "hotel_name", "type": "string"},
        {"name": "city", "type": "string"},
        {"name": "available_rooms", "type": "int"},
        {"name": "price_per_night", "type": "double"},
        {"name": "currency", "type": "string"},
        {"name": "event_ts", "type": "long"}
    ]
})


# Schema Registry connection
sr_config = {
    "url": os.environ["SCHEMA_REGISTRY_URL"],
    "basic.auth.user.info": (
        os.environ["SCHEMA_REGISTRY_API_KEY"] + ":" +
        os.environ["SCHEMA_REGISTRY_API_SECRET"]
    )
}

schema_registry = SchemaRegistryClient(sr_config)

avro_serializer = AvroSerializer(
    schema_registry,
    schema_str,
    lambda obj, ctx: obj
)


# Kafka producer
producer = SerializingProducer({
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer
})


app = FastAPI(title="Airline Hotel Inventory Webhook")


class HotelUpdate(BaseModel):
    hotel_id: str
    hotel_name: str
    city: str
    available_rooms: int = Field(ge=0)
    price_per_night: float = Field(gt=0)
    currency: str = "INR"


@app.get("/health")
def health():
    return {
        "status": "healthy",
        "service": "hotel-webhook",
        "kafka_topic": TOPIC
    }


@app.post("/webhook/hotels")
def receive_hotel_update(data: HotelUpdate):

    record = data.model_dump()
    record["event_ts"] = int(time.time() * 1000)

    try:
        producer.produce(
            topic=TOPIC,
            key=record["hotel_id"],
            value=record,
            on_delivery=lambda err, msg: (
                print(f"Delivery failed: {err}")
                if err else None
            )
        )

        remaining = producer.flush(15)

        if remaining:
            raise RuntimeError(
                f"{remaining} Kafka message(s) still pending"
            )

        return {
            "status": "published",
            "topic": TOPIC,
            "hotel_id": record["hotel_id"],
            "available_rooms": record["available_rooms"],
            "price_per_night": record["price_per_night"],
            "event_ts": record["event_ts"]
        }

    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Kafka publishing failed: {str(exc)}"
        )
