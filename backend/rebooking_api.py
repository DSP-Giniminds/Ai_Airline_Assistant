
import os
import time
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import (
    SerializingProducer,
    Consumer,
    KafkaError,
    TopicPartition
)
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField
)
from confluent_kafka.schema_registry.avro import (
    AvroSerializer,
    AvroDeserializer
)
load_dotenv("/home/airline-ai/.env")

TOPIC = "rebooking_events_stream_v2"

required = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET"
]

missing = [k for k in required if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing environment variables: {missing}")

key_schema = """
{
  "type": "record",
  "name": "rebooking_events_stream_key",
  "namespace": "org.apache.flink.avro.generated.record",
  "fields": [
    {"name": "request_id", "type": "string"}
  ]
}
"""

schema = """
{
  "type": "record",
  "name": "rebooking_events_stream_value",
  "namespace": "org.apache.flink.avro.generated.record",
  "fields": [
    {"name": "booking_id", "type": "string"},
    {"name": "passenger_id", "type": "string"},
    {"name": "original_flight_id", "type": "string"},
    {"name": "action", "type": "string"},
    {"name": "target_flight_id", "type": ["null", "string"], "default": null},
    {"name": "hotel_id", "type": ["null", "string"], "default": null},
    {"name": "status", "type": "string"},
    {"name": "approved_by", "type": ["null", "string"], "default": null},
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
    "key.serializer": AvroSerializer(
    sr, key_schema, lambda obj, ctx: {"request_id": obj}
),
    "value.serializer": AvroSerializer(sr, schema, lambda obj, ctx: obj)
})

app = FastAPI(title="Airline Rebooking API")

class RebookingRequest(BaseModel):
    request_id: str | None = None
    booking_id: str
    passenger_id: str
    original_flight_id: str
    action: str = Field(pattern="^(REBOOK|HOTEL|BOTH)$")
    target_flight_id: str | None = None
    hotel_id: str | None = None
    approved_by: str

def publish_event(record):
    try:
        producer.produce(
            topic=TOPIC,
            key=record["request_id"],
            value={k: v for k, v in record.items() if k != "request_id"}
        )
        remaining = producer.flush(15)
        if remaining:
            raise RuntimeError("Kafka delivery timed out")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

@app.get("/health")
def health():
    return {"status": "healthy", "service": "rebooking-api"}


@app.post("/rebooking/request")
def create_request(data: RebookingRequest):

    # Read existing request history from Kafka
    existing_events = read_kafka_topic(
        topic=TOPIC,
        key_format="avro",
        latest_by_key=False,
        limit=50000
    )

    # Find requests for the same booking, passenger, flight and action
    matching = [
        event for event in existing_events
        if event.get("booking_id") == data.booking_id
        and event.get("passenger_id") == data.passenger_id
        and event.get("original_flight_id") == data.original_flight_id
        and event.get("action") == data.action
    ]

    # Check the latest status
    latest = max(
        matching,
        key=lambda event: int(event.get("event_ts") or 0),
        default=None
    )

    if latest and latest.get("status") in (
        "PENDING_APPROVAL", "APPROVED", "REJECTED"
    ):
        key = latest.get("_key")
        existing_id = key.get("request_id") if isinstance(key, dict) else None

        raise HTTPException(
            status_code=409,
            detail={
                "code": "REQUEST_ALREADY_EXISTS",
                "message": "This action has already been requested and cannot be repeated.",
                "existing_status": latest.get("status"),
                "request_id": existing_id
            }
        )

    request_id = str(uuid.uuid4())

    record = {
        "request_id": request_id,
        "booking_id": data.booking_id,
        "passenger_id": data.passenger_id,
        "original_flight_id": data.original_flight_id,
        "action": data.action,
        "target_flight_id": data.target_flight_id,
        "hotel_id": data.hotel_id,
        "status": "PENDING_APPROVAL",
        "approved_by": None,
        "event_ts": int(time.time() * 1000)
    }

    publish_event(record)
    return {"status": "published", **record}

@app.post("/rebooking/approve")
def approve_request(data: RebookingRequest):
    if not data.approved_by.strip():
        raise HTTPException(status_code=400, detail="Approver required")

    record = {
        "request_id": data.request_id or str(uuid.uuid4()),
        "booking_id": data.booking_id,
        "passenger_id": data.passenger_id,
        "original_flight_id": data.original_flight_id,
        "action": data.action,
        "target_flight_id": data.target_flight_id,
        "hotel_id": data.hotel_id,
        "status": "APPROVED",
        "approved_by": data.approved_by,
        "event_ts": int(time.time() * 1000)
    }

    publish_event(record)
    return {"status": "approval_event_published", **record}

@app.post("/rebooking/reject")
def reject_request(data: RebookingRequest):
    record = {
        "request_id": data.request_id or str(uuid.uuid4()),
        "booking_id": data.booking_id,
        "passenger_id": data.passenger_id,
        "original_flight_id": data.original_flight_id,
        "action": data.action,
        "target_flight_id": data.target_flight_id,
        "hotel_id": data.hotel_id,
        "status": "REJECTED",
        "approved_by": data.approved_by,
        "event_ts": int(time.time() * 1000)
    }

    publish_event(record)
    return {"status": "rejection_event_published", **record}

# ============================================================
# DYNAMIC KAFKA READ APIs
# ============================================================

value_deserializer = AvroDeserializer(sr)
key_deserializer = AvroDeserializer(sr)


def read_kafka_topic(
    topic,
    key_format="avro",
    latest_by_key=False,
    limit=100
):
    """
    Read records directly from Kafka.

    latest_by_key=True:
        Scan retained records and return the latest value per key.
        Used for state topics.

    latest_by_key=False:
        Read recent events from the topic.
        Used for event-history topics.
    """

    consumer = Consumer({
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["KAFKA_API_KEY"],
        "sasl.password": os.environ["KAFKA_API_SECRET"],
        "group.id": f"airline-api-reader-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "enable.partition.eof": True
    })

    records = []
    latest_records = {}

    try:
        metadata = consumer.list_topics(topic, timeout=10)

        if topic not in metadata.topics:
            raise RuntimeError(f"Kafka topic not found: {topic}")

        topic_metadata = metadata.topics[topic]

        if topic_metadata.error is not None:
            raise RuntimeError(str(topic_metadata.error))

        assignments = []
        high_offsets = {}

        for partition_id in topic_metadata.partitions:
            partition = TopicPartition(topic, partition_id)

            low, high = consumer.get_watermark_offsets(
                partition,
                timeout=10
            )

            high_offsets[partition_id] = high

            if high <= low:
                continue

            if latest_by_key:
                start_offset = low
            else:
                start_offset = max(low, high - limit)

            assignments.append(
                TopicPartition(topic, partition_id, start_offset)
            )

        if not assignments:
            return []

        consumer.assign(assignments)

        completed = set()
        deadline = time.time() + 30
        idle_count = 0
        max_scan_records = 50000

        while len(completed) < len(assignments):
            if time.time() > deadline:
                break

            if latest_by_key and len(records) >= max_scan_records:
                break

            message = consumer.poll(0.5)

            if message is None:
                idle_count += 1

                if idle_count >= 12:
                    break

                continue

            idle_count = 0

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    completed.add(message.partition())
                    continue

                raise RuntimeError(str(message.error()))

            partition_id = message.partition()

            if message.offset() >= high_offsets[partition_id] - 1:
                completed.add(partition_id)

            if message.value() is None:
                continue

            # Deserialize Avro value
            value = value_deserializer(
                message.value(),
                SerializationContext(topic, MessageField.VALUE)
            )

            if value is None:
                continue

            # Deserialize key according to the topic's key format
            if message.key() is None:
                key = None

            elif key_format == "avro":
                key = key_deserializer(
                    message.key(),
                    SerializationContext(topic, MessageField.KEY)
                )

            else:
                key = message.key().decode("utf-8")

            record = dict(value)
            record["_key"] = key

            if latest_by_key:
                if isinstance(key, dict):
                    key_id = tuple(sorted(key.items()))
                else:
                    key_id = key

                if key_id is None:
                    key_id = record.get(
                        "flight_id",
                        record.get("hotel_id")
                    )

                if key_id is not None:
                    previous = latest_records.get(key_id)

                    current_ts = record.get("event_ts", 0)
                    previous_ts = (
                        previous.get("event_ts", 0)
                        if previous else -1
                    )

                    if current_ts >= previous_ts:
                        latest_records[key_id] = record
            else:
                records.append(record)

        if latest_by_key:
            records = list(latest_records.values())

        records.sort(
            key=lambda item: item.get("event_ts", 0),
            reverse=True
        )

        return records

    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Kafka read failed for {topic}: {str(exc)}"
        )

    finally:
        consumer.close()


# ============================================================
# FLIGHT DATA
# ============================================================

@app.get("/flights")
def get_flights():
    data = read_kafka_topic(
        topic="flight_status",
        key_format="string",
        latest_by_key=True
    )

    return {
        "count": len(data),
        "data": data
    }



# ============================================================
# ALTERNATIVE FLIGHT DISCOVERY
# ============================================================

from datetime import datetime

@app.get("/alternative-flights")
def get_alternative_flights(
    origin: str = "BOM",
    destination: str = "DEL",
    min_seats: int = 1,
    limit: int = 100
):
    origin = origin.strip().upper()
    destination = destination.strip().upper()
    limit = max(1, min(limit, 500))

    data = read_kafka_topic(
        topic="alternative_flights",
        key_format="string",
        latest_by_key=True,
        limit=50000
    )

    now = datetime.now().astimezone()
    matching = []

    for flight in data:
        try:
            departure = datetime.fromisoformat(
                flight["departure_time"]
            )

            if departure.tzinfo is None:
                continue

            if (
                flight.get("origin", "").upper() != origin
                or flight.get("destination", "").upper() != destination
                or int(flight.get("available_seats", 0)) < min_seats
                or flight.get("status") not in ("SCHEDULED", "DELAYED")
                or departure <= now
            ):
                continue

            matching.append(flight)

        except (ValueError, TypeError, KeyError):
            continue

    matching.sort(
        key=lambda f: (
            datetime.fromisoformat(f["departure_time"]),
            float(f.get("price", 0))
        )
    )

    return {
        "count": len(matching[:limit]),
        "origin": origin,
        "destination": destination,
        "data": matching[:limit]
    }


# ============================================================
# PASSENGER CONNECTION RISK
# ============================================================

@app.get("/passengers")
def get_passengers():
    data = read_kafka_topic(
        topic="passenger_state_stream",
        key_format="avro",
        latest_by_key=True
    )

    return {
        "count": len(data),
        "data": data
    }


# ============================================================
# AI RECOVERY RECOMMENDATIONS
# ============================================================

@app.get("/recommendations")
def get_recommendations(limit: int = 100):
    limit = max(1, min(limit, 500))

    data = read_kafka_topic(
        topic="ai_recovery_recommendations",
        key_format="avro",
        latest_by_key=False,
        limit=limit
    )

    return {
        "count": len(data),
        "data": data
    }


# ============================================================
# HOTEL INVENTORY
# ============================================================

@app.get("/hotels")
def get_hotels():
    data = read_kafka_topic(
        topic="hotel_inventory",
        key_format="string",
        latest_by_key=True
    )

    return {
        "count": len(data),
        "data": data
    }


# ============================================================
# REBOOKING EVENT HISTORY
# ============================================================

@app.get("/rebooking/requests")
def get_rebooking_requests(limit: int = 100):
    limit = max(1, min(limit, 500))

    data = read_kafka_topic(
        topic="rebooking_events_stream_v2",
        key_format="avro",
        latest_by_key=True,
        limit=limit
    )

    return {
        "count": len(data),
        "data": data
    }


# ============================================================
# PASSENGER-SPECIFIC BOOKING LOOKUP
# ============================================================

@app.get("/passenger/booking")
def passenger_booking(booking_id: str, passenger_id: str):
    booking_id = booking_id.strip()
    passenger_id = passenger_id.strip()

    if not booking_id or not passenger_id:
        raise HTTPException(
            status_code=400,
            detail="Booking reference and passenger ID are required"
        )

    passengers = read_kafka_topic(
        "passenger_state_stream",
        key_format="avro",
        latest_by_key=True
    )

    passenger = next(
        (
            p for p in passengers
            if p.get("booking_id") == booking_id
            and p.get("_key", {}).get("passenger_id") == passenger_id
        ),
        None
    )

    if not passenger:
        raise HTTPException(
            status_code=404,
            detail="Booking or passenger details not found"
        )

    flights = read_kafka_topic(
        "flight_status",
        key_format="string",
        latest_by_key=True
    )

    recommendations = read_kafka_topic(
        "ai_recovery_recommendations",
        key_format="avro",
        limit=100
    )

    hotels = read_kafka_topic(
        "hotel_inventory",
        key_format="string",
        latest_by_key=True
    )

    rebookings = read_kafka_topic(
        "rebooking_events_stream_v2",
        key_format="avro",
        limit=100
    )

    flight = next(
        (f for f in flights if f.get("flight_id") == passenger.get("flight_id")),
        None
    )

    passenger_recommendations = [
        r for r in recommendations
        if r.get("booking_id") == booking_id
        and r.get("passenger_id") == passenger_id
    ]

    passenger_rebookings = [
        r for r in rebookings
        if r.get("booking_id") == booking_id
        and r.get("passenger_id") == passenger_id
    ]

    passenger_rebookings.sort(
        key=lambda r: r.get("event_ts", 0),
        reverse=True
    )

    return {
        "passenger": passenger,
        "flight": flight,
        "recommendations": passenger_recommendations,
        "hotels": hotels,
        "rebooking_events": passenger_rebookings
    }

# ============================================================
# CONFLUENT MCP - PASSENGER CHAT API
# ============================================================

import json
import base64
import urllib.request
import urllib.error
import re

load_dotenv("/home/airline-ai/config/mcp.env", override=True)

MCP_ENDPOINT = os.getenv("MCP_ENDPOINT")
MCP_API_KEY = os.getenv("MCP_API_KEY")
MCP_API_SECRET = os.getenv("MCP_API_SECRET")


def mcp_call(tool_name, arguments):
    if not all([MCP_ENDPOINT, MCP_API_KEY, MCP_API_SECRET]):
        raise HTTPException(status_code=500, detail="MCP configuration missing")

    credentials = base64.b64encode(
        f"{MCP_API_KEY}:{MCP_API_SECRET}".encode()
    ).decode()

    payload = {
        "jsonrpc": "2.0",
        "id": 100,
        "method": "tools/call",
        "params": {
            "name": tool_name,
            "arguments": arguments
        }
    }

    request = urllib.request.Request(
        MCP_ENDPOINT,
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Basic {credentials}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream"
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode()

        # Confluent MCP returns Server-Sent Events.
        data_lines = [
            line[6:].strip()
            for line in raw.splitlines()
            if line.startswith("data: ")
        ]

        if not data_lines:
            raise ValueError("MCP returned no response data")

        result = json.loads(data_lines[-1])

        if "error" in result:
            raise ValueError(str(result["error"]))

        contents = result.get("result", {}).get("content", [])

        for item in contents:
            if item.get("type") == "text":
                return json.loads(item["text"])

        raise ValueError("No text result returned by MCP")

    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"MCP request failed: {str(exc)}"
        )


def mcp_query(topic, sql):
    # Step 1: Discover topics.
    topic_result = mcp_call("listTopics", {})

    available = [
        item["name"]
        for item in topic_result.get("rows", {}).get("topics", [])
    ]

    if topic not in available:
        raise HTTPException(status_code=404, detail=f"Topic unavailable: {topic}")

    # Step 2: Retrieve schema before querying.
    metadata = mcp_call("getMetadata", {"topic_name": topic})

    if metadata.get("status") != "success":
        raise HTTPException(status_code=502, detail="Unable to retrieve topic schema")

    # Step 3: Query live data.
    result = mcp_call("queryData", {
        "topic_name": topic,
        "query": sql,
        "max_result_rows": 10
    })

    if result.get("status") != "success":
        raise HTTPException(
            status_code=502,
            detail=f"MCP query unsuccessful. Actual response: {json.dumps(result, default=str)[:3000]}"
        )

    rows = result.get("rows", {})
    columns = [
        column["name"]
        for column in rows.get("schema", {}).get("columns", [])
    ]

    return [
        dict(zip(columns, item.get("row", [])))
        for item in rows.get("data", [])
    ]


class ChatRequest(BaseModel):
    booking_id: str
    passenger_id: str
    message: str


@app.post("/chat")
def passenger_chat(request: ChatRequest):

    booking_id = request.booking_id.strip().upper()
    passenger_id = request.passenger_id.strip().upper()
    message = request.message.strip()

    if not re.fullmatch(r"[A-Z0-9-]{2,30}", booking_id):
        raise HTTPException(status_code=400, detail="Invalid booking reference")

    if not re.fullmatch(r"[A-Z0-9-]{2,30}", passenger_id):
        raise HTTPException(status_code=400, detail="Invalid passenger ID")

    if not message or len(message) > 1000:
        raise HTTPException(status_code=400, detail="Enter a valid message")

    # Retrieve the passenger's live state.
    passenger_sql = (
        'SELECT "PASSENGER_ID", "BOOKING_ID", "FLIGHT_ID", '
        '"NEXT_FLIGHT_ID", "CONNECTION_AIRPORT", "FLIGHT_STATUS", '
        '"DELAY_MINUTES", "REMAINING_CONNECTION_MINUTES", "CONNECTION_RISK" '
        'FROM passenger_state_stream '
        f'WHERE "BOOKING_ID" = \'{booking_id}\' '
        f'AND "PASSENGER_ID" = \'{passenger_id}\' LIMIT 5'
    )

    passengers = mcp_query("passenger_state_stream", passenger_sql)

    if not passengers:
        raise HTTPException(
            status_code=404,
            detail="No matching passenger booking found"
        )

    passenger = passengers[0]
    flight_id = passenger["FLIGHT_ID"]

    # Resolve the flight using the passenger's flight ID.
    flight_sql = (
        'SELECT "FLIGHT_ID", "AIRLINE", "ORIGIN", "DESTINATION", '
        '"STATUS", "DELAY_MINUTES", "SCHEDULED_ARRIVAL", '
        '"ESTIMATED_ARRIVAL", "GATE", "DELAY_REASON" '
        'FROM flight_status_v2 '
        f'WHERE "FLIGHT_ID" = \'{flight_id}\' LIMIT 5'
    )

    flights = mcp_query("flight_status_v2", flight_sql)

    # Retrieve available hotel inventory.
    hotel_sql = (
        'SELECT "HOTEL_ID", "HOTEL_NAME", "CITY", '
        '"AVAILABLE_ROOMS", "PRICE_PER_NIGHT", "CURRENCY" '
        'FROM hotel_inventory LIMIT 10'
    )

    hotels = mcp_query("hotel_inventory", hotel_sql)

    flight = flights[0] if flights else {}

    # POC response generation from actual retrieved data.
    response_text = (
        f"Hello! I checked your booking {booking_id}.\n\n"
        f"Your flight: {flight.get('FLIGHT_ID', flight_id)} "
        f"({flight.get('AIRLINE', 'Airline unavailable')})\n"
        f"Route: {flight.get('ORIGIN', '—')} to "
        f"{flight.get('DESTINATION', '—')}\n"
        f"Flight status: {flight.get('STATUS', 'Unavailable')}\n"
        f"Delay: {flight.get('DELAY_MINUTES', '—')} minutes\n"
        f"Gate: {flight.get('GATE', '—')}\n\n"
        f"Connection flight: {passenger.get('NEXT_FLIGHT_ID', '—')}\n"
        f"Connection risk: {passenger.get('CONNECTION_RISK', '—')}\n"
        f"Remaining connection time: "
        f"{passenger.get('REMAINING_CONNECTION_MINUTES', '—')} minutes.\n\n"
        "Available hotel options:\n"
    )

    if hotels:
        for hotel in hotels:
            response_text += (
                f"\n{hotel['HOTEL_NAME']} - {hotel['CITY']}: "
                f"{hotel['AVAILABLE_ROOMS']} rooms available, "
                f"{hotel['CURRENCY']} {hotel['PRICE_PER_NIGHT']} per night."
            )
    else:
        response_text += "\nNo hotel inventory currently available."

    response_text += (
        "\n\nI can help you request rebooking or hotel assistance. "
        "Any request will be submitted for airline operations review."
    )

    return {
        "status": "success",
        "booking_id": booking_id,
        "passenger_id": passenger_id,
        "reply": watsonx_reply(message, passenger, flight, hotels),
        "context": {
            "passenger": passenger,
            "flight": flight,
            "hotels": hotels
        }
    }


# ============================================================

# ============================================================
# GROQ AI - AIRLINEIQ CHAT INTEGRATION
# ============================================================

def get_granite_model():
    """
    Retains the existing function name to avoid modifying
    the existing chat route. Returns a Groq client.
    """
    from groq import Groq

    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise HTTPException(
            status_code=500,
            detail="Groq API configuration is missing"
        )

    return Groq(
        api_key=api_key,
        timeout=35.0,
        max_retries=1
    )


def groq_chat(model, messages, max_tokens=500):
    """
    Common Groq completion helper.
    Uses max_completion_tokens for GPT-OSS models.
    """
    model_id = os.getenv(
        "GROQ_MODEL_ID",
        "openai/gpt-oss-20b"
    )

    try:
        result = model.chat.completions.create(
            model=model_id,
            messages=messages,
            max_completion_tokens=max_tokens
        )

        content = result.choices[0].message.content

        if not content:
            raise HTTPException(
                status_code=502,
                detail="The AI model returned an empty response"
            )

        return content.strip()

    except HTTPException:
        raise

    except Exception as exc:
        error_status = getattr(exc, "status_code", None)

        if error_status == 429:
            detail = (
                "The AI service has reached its usage limit. "
                "Please try again shortly."
            )
        elif error_status in (401, 403):
            detail = (
                "The AI service authentication or access "
                "configuration needs attention."
            )
        else:
            detail = (
                "The AI service is temporarily unavailable. "
                "Please try again."
            )

        print(
            f"Groq API error: status={error_status}, "
            f"type={type(exc).__name__}"
        )

        raise HTTPException(
            status_code=502,
            detail=detail
        ) from exc


def watsonx_reply(message, passenger, flight, hotels):
    """
    Legacy passenger-assistance function.
    Uses Groq instead of IBM watsonx.
    """
    model = get_granite_model()

    live_context = {
        "passenger": passenger,
        "flight": flight,
        "hotels": hotels
    }

    messages = [
        {
            "role": "system",
            "content": (
                "You are AirlineIQ, an airline passenger assistance AI. "
                "Answer using only the supplied live airline data. "
                "Explain flight delays, connection risks and available hotels. "
                "Never invent flights, prices, availability or policies. "
                "Never claim that a ticket or hotel has been booked. "
                "Rebooking and hotel requests require airline operations approval. "
                "Be friendly, concise and helpful."
            )
        },
        {
            "role": "user",
            "content": (
                f"Passenger question: {message}\n\n"
                f"Verified live airline information:\n"
                f"{json.dumps(live_context, default=str)}"
            )
        }
    ]

    return groq_chat(model, messages, max_tokens=500)


def classify_passenger_intent(model, message):

    prompt = f"""
Classify the passenger's message into exactly one category.

Categories:
general - greetings, small talk and unrelated general questions
flight - flight status, delay, arrival, departure or gate questions
connection - connecting flight, connection time or connection risk
hotel - hotel availability, accommodation or hotel prices
alternative_flights - requests to find, show, compare or check alternative flights
recovery - rebooking, missed flights or requests for travel assistance

Return ONLY valid JSON in this format:
{{"intent":"general"}}

Passenger message: {message}
"""

    content = groq_chat(
        model,
        [{"role": "user", "content": prompt}],
        max_tokens=150
    )

    try:
        match = re.search(r"\{.*?\}", content, re.DOTALL)
        intent = (
            json.loads(match.group(0))["intent"]
            if match else "general"
        )
    except (ValueError, TypeError, KeyError):
        intent = "general"

    allowed = {
        "general",
        "flight",
        "connection",
        "hotel",
        "alternative_flights",
        "recovery"
    }

    return intent if intent in allowed else "general"


def generate_agent_response(model, message, intent, context):

    system_prompt = """
You are AirlineIQ, a professional and friendly airline passenger AI.

Rules:
1. Answer the passenger's actual question.
2. For greetings and general conversation, respond naturally.
3. Never introduce flight information unless relevant to the question.
4. Use only the verified information supplied in the context.
5. Never invent flight delay reasons, alternative flights,
   prices, hotel availability or airline policies.
6. If a requested detail is unavailable, say so clearly.
7. Never claim that a ticket or hotel has been booked.
8. Rebooking and hotel assistance require airline operations approval.
9. Keep responses concise, natural and conversational.
10. Never reveal internal APIs, Kafka, MCP or technical implementation.
11. When asked why a flight is delayed, use the DELAY_REASON
    from the verified flight context. Explain it clearly.
    If DELAY_REASON is null or unavailable, say that the specific
    reason has not been provided. Never guess.
12. When alternative flights are supplied, show their flight ID,
    airline, departure time, arrival time, available seats and price.
    Never invent or modify these values.
13. Do not claim an alternative flight is booked or reserved.
"""

    messages = [
        {
            "role": "system",
            "content": system_prompt
        },
        {
            "role": "user",
            "content": (
                f"Passenger question: {message}\n\n"
                f"Intent: {intent}\n\n"
                f"Relevant verified information:\n"
                f"{json.dumps(context, default=str)}"
            )
        }
    ]

    return groq_chat(
        model,
        messages,
        max_tokens=500
    )


pending_chat_actions = {}

# Conversational self-service booking state, keyed by passenger session.

booking_chat_sessions = {}
hotel_chat_sessions = {}


def process_hotel_chat_session(session_key, message, booking_id, passenger_id):
    """Manage the conversational hotel booking workflow."""

    session = hotel_chat_sessions.get(session_key)
    if not session:
        return None

    text = message.strip()
    normalized = text.lower()
    stage = session.get("stage")

    # Handle greetings before processing hotel-selection input.
    greeting = normalized.strip(" .,!?:;\\t")
    greeting_messages = {
        "hi", "hello", "hey", "hi there", "hello there",
        "good morning", "good afternoon", "good evening"
    }

    if greeting in greeting_messages:
        return {
            "status": "success",
            "intent": "greeting",
            "reply": (
                "Hello! Welcome to the Airline AI Assistant. "
                "How can I help you with your flight, alternative flights, "
                "or hotel arrangements?"
            )
        }

    # STEP 1: Passenger selects a hotel.
    if stage == "SELECT_HOTEL":

        selection = text.strip()

        # Remove common conversational prefixes.
        selection = re.sub(
            r"^(?:(?:i\s+want\s+to|i\s+would\s+like\s+to|"
            r"i'd\s+like\s+to|please)\s+)?"
            r"(?:book|reserve|select|choose)\s+",
            "",
            selection,
            flags=re.IGNORECASE
        ).strip(" .,!?")

        hotels = session.get("hotels", [])
        selected = None

        # Accept: "2", "2.", "2. DoubleTree...", or the hotel name.
        number_match = re.match(
            r"^(\d+)\s*[.):\-]?\s*(.*)$",
            selection
        )

        if number_match:
            index = int(number_match.group(1)) - 1
            if 0 <= index < len(hotels):
                selected = hotels[index]

        if selected is None and selection.isdigit():
            index = int(selection) - 1
            if 0 <= index < len(hotels):
                selected = hotels[index]
        else:
            exact = [
                h for h in hotels
                if h["name"].lower() == selection.lower()
            ]

            if len(exact) == 1:
                selected = exact[0]
            else:
                matches = [
                    h for h in hotels
                    if selection.lower() in h["name"].lower()
                ]

                if len(matches) == 1:
                    selected = matches[0]

                elif len(matches) > 1:
                    return {
                        "status": "success",
                        "intent": "hotel_selection",
                        "reply": "Multiple hotels match that name. Please enter the full hotel name or its list number."
                    }

        if not selected:
            return {
                "status": "success",
                "intent": "hotel_selection",
                "reply": "I couldn't find that hotel in the displayed list. Please enter its name or list number."
            }

        inventory = ensure_osm_demo_inventory(
            selected,
            session["airport_code"]
        )

        room_types = inventory.get("room_types") or []

        if isinstance(room_types, str):
            room_types = json.loads(room_types)

        if not room_types:
            return {
                "status": "error",
                "intent": "hotel_selection",
                "reply": "No room types are available in the demo inventory for this hotel."
            }

        session.update({
            "stage": "SELECT_ROOM",
            "hotel_id": inventory["hotel_id"],
            "hotel_name": inventory["hotel_name"],
            "room_types": room_types
        })

        lines = []

        for i, room in enumerate(room_types, 1):
            price = float(
                room.get("price", inventory["price_per_night"])
            )
            available = int(
                room.get("available_rooms", inventory["available_rooms"])
            )

            lines.append(
                f"{i}. {room['name']} - INR {price:,.2f} per night - "
                f"{available} demo rooms"
            )

        return {
            "status": "success",
            "intent": "hotel_room_selection",
            "reply": (
                f"You selected {inventory['hotel_name']}.\n\n"
                "Available DEMO room options:\n"
                + "\n".join(lines)
                + "\n\nSelect a room type by entering its name or number."
            )
        }

    # STEP 2: Passenger selects room type.
    if stage == "SELECT_ROOM":

        rooms = session["room_types"]
        selected_room = None

        if text.isdigit():
            index = int(text) - 1
            if 0 <= index < len(rooms):
                selected_room = rooms[index]
        else:
            matches = [
                r for r in rooms
                if r["name"].lower() == normalized
            ]

            if len(matches) == 1:
                selected_room = matches[0]
            else:
                matches = [
                    r for r in rooms
                    if normalized in r["name"].lower()
                ]

                if len(matches) == 1:
                    selected_room = matches[0]

        if not selected_room:
            return {
                "status": "success",
                "intent": "hotel_room_selection",
                "reply": "Please enter one of the displayed room names or its number."
            }

        session["room_type"] = selected_room["name"]
        session["room_price"] = float(selected_room["price"])
        session["room_available"] = int(
            selected_room.get("available_rooms", 0)
        )
        session["stage"] = "ENTER_ROOMS"

        return {
            "status": "success",
            "intent": "hotel_room_selection",
            "reply": (
                f"Selected {session['room_type']}.\n"
                f"Price: INR {session['room_price']:,.2f} per night.\n"
                f"Demo rooms available: {session['room_available']}.\n\n"
                "How many rooms would you like? Enter 1 to 5."
            )
        }

    # STEP 3: Number of rooms.
    if stage == "ENTER_ROOMS":

        match = re.search(r"\d+", text)

        if not match:
            return {
                "status": "success",
                "intent": "hotel_booking_details",
                "reply": "Please enter the number of rooms, from 1 to 5."
            }

        rooms = int(match.group())

        if rooms < 1 or rooms > 5:
            return {
                "status": "success",
                "intent": "hotel_booking_details",
                "reply": "Please select between 1 and 5 rooms."
            }

        if rooms > session["room_available"]:
            return {
                "status": "success",
                "intent": "hotel_booking_details",
                "reply": f"Only {session['room_available']} demo rooms are available. Please select a smaller quantity."
            }

        session["rooms"] = rooms
        session["stage"] = "ENTER_GUESTS"

        return {
            "status": "success",
            "intent": "hotel_booking_details",
            "reply": "How many guests will be staying? Enter 1 to 10."
        }

    # STEP 4: Number of guests.
    if stage == "ENTER_GUESTS":

        match = re.search(r"\d+", text)

        if not match:
            return {
                "status": "success",
                "intent": "hotel_booking_details",
                "reply": "Please enter a valid guest count from 1 to 10."
            }

        guests = int(match.group())

        if guests < 1 or guests > 10:
            return {
                "status": "success",
                "intent": "hotel_booking_details",
                "reply": "The guest count must be between 1 and 10."
            }

        session["guests"] = guests
        session["stage"] = "ENTER_CHECKIN"

        return {
            "status": "success",
            "intent": "hotel_booking_dates",
            "reply": "Please enter your check-in date in YYYY-MM-DD format."
        }

    # STEP 5: Check-in and check-out dates.
    if stage in ("ENTER_CHECKIN", "ENTER_CHECKOUT"):

        try:
            parsed_date = hotel_date.fromisoformat(text)
        except (ValueError, TypeError):
            return {
                "status": "success",
                "intent": "hotel_booking_dates",
                "reply": "Invalid date. Please use YYYY-MM-DD format."
            }

        if parsed_date < hotel_date.today():
            return {
                "status": "success",
                "intent": "hotel_booking_dates",
                "reply": "That date is in the past. Please enter a future date."
            }

        if stage == "ENTER_CHECKIN":

            session["check_in"] = parsed_date.isoformat()
            session["stage"] = "ENTER_CHECKOUT"

            return {
                "status": "success",
                "intent": "hotel_booking_dates",
                "reply": "Please enter your check-out date in YYYY-MM-DD format."
            }

        check_in = hotel_date.fromisoformat(session["check_in"])

        if parsed_date <= check_in:
            return {
                "status": "success",
                "intent": "hotel_booking_dates",
                "reply": "Check-out must be after check-in. Please enter another date."
            }

        nights = (parsed_date - check_in).days
        total = (
            session["room_price"]
            * nights
            * session["rooms"]
        )

        session["check_out"] = parsed_date.isoformat()
        session["nights"] = nights
        session["total_amount"] = total
        session["stage"] = "CONFIRM_HOTEL_BOOKING"

        hotel_booking_payload = {
            "booking_id": booking_id,
            "passenger_id": passenger_id,
            "hotel_id": session["hotel_id"],
            "room_type": session["room_type"],
            "check_in": session["check_in"],
            "check_out": session["check_out"],
            "guests": session["guests"],
            "rooms": session["rooms"]
        }

        # Find all confirmed reservations overlapping these dates.
        conflict_conn = booking_db()

        try:
            conflict_cur = conflict_conn.cursor(
                cursor_factory=RealDictCursor
            )

            conflict_cur.execute("""
                SELECT booking_ref, hotel_name, room_type,
                       check_in, check_out, rooms
                FROM hotel_bookings
                WHERE booking_id = %s
                  AND passenger_id = %s
                  AND booking_status = 'CONFIRMED'
                  AND check_in < %s
                  AND check_out > %s
                ORDER BY created_at DESC
            """, (
                booking_id,
                passenger_id,
                parsed_date,
                check_in
            ))

            conflicts = [
                dict(row) for row in conflict_cur.fetchall()
            ]

        finally:
            conflict_conn.close()

        if conflicts:
            for conflict in conflicts:
                conflict["check_in"] = str(conflict["check_in"])
                conflict["check_out"] = str(conflict["check_out"])

            if len(conflicts) == 1:
                conflict = conflicts[0]

                pending_chat_actions[session_key] = {
                    "action": "HOTEL_REPLACEMENT_CONFIRMATION",
                    "hotel_booking": hotel_booking_payload,
                    "old_booking_ref": conflict["booking_ref"]
                }

                return {
                    "status": "confirmation_required",
                    "intent": "hotel_replacement_confirmation",
                    "reply": (
                        "You already have an overlapping confirmed hotel reservation.\n\n"
                        f"Existing hotel: {conflict['hotel_name']}\n"
                        f"Existing booking reference: {conflict['booking_ref']}\n"
                        f"Existing dates: {conflict['check_in']} to {conflict['check_out']}\n\n"
                        f"New hotel: {session['hotel_name']}\n"
                        f"New dates: {session['check_in']} to {session['check_out']}\n"
                        f"New total: INR {total:,.2f}\n\n"
                        "Do you want to cancel the existing reservation and "
                        "create this new reservation?\n"
                        "Reply Yes to replace it or No to keep the existing reservation."
                    )
                }

            pending_chat_actions[session_key] = {
                "action": "HOTEL_REPLACEMENT_SELECT",
                "hotel_booking": hotel_booking_payload,
                "conflicts": conflicts
            }

            conflict_lines = []

            for index, conflict in enumerate(conflicts, start=1):
                conflict_lines.append(
                    f"{index}. {conflict['hotel_name']} - "
                    f"{conflict['booking_ref']} - "
                    f"{conflict['check_in']} to {conflict['check_out']} "
                    f"({conflict['room_type']}, {conflict['rooms']} rooms)"
                )

            return {
                "status": "confirmation_required",
                "intent": "hotel_replacement_selection",
                "reply": (
                    "You have multiple confirmed hotel reservations "
                    "overlapping your requested dates:\n\n"
                    + "\n".join(conflict_lines)
                    + "\n\nNew hotel: "
                    + session["hotel_name"]
                    + f"\nNew dates: {session['check_in']} to {session['check_out']}"
                    + f"\nNew total: INR {total:,.2f}"
                    + "\n\nWhich existing reservation would you like to replace? "
                    "Enter its number or booking reference. "
                    "Reply No to keep all existing reservations."
                )
            }

        pending_chat_actions[session_key] = {
            "action": "HOTEL_BOOKING",
            "hotel_booking": {
                "booking_id": booking_id,
                "passenger_id": passenger_id,
                "hotel_id": session["hotel_id"],
                "room_type": session["room_type"],
                "check_in": session["check_in"],
                "check_out": session["check_out"],
                "guests": session["guests"],
                "rooms": session["rooms"]
            }
        }

        return {
            "status": "confirmation_required",
            "intent": "hotel_booking_confirmation",
            "reply": (
                "Please review your SIMULATED hotel reservation:\n\n"
                f"Hotel: {session['hotel_name']}\n"
                f"Room: {session['room_type']}\n"
                f"Rooms: {session['rooms']}\n"
                f"Guests: {session['guests']}\n"
                f"Check-in: {session['check_in']}\n"
                f"Check-out: {session['check_out']}\n"
                f"Nights: {nights}\n"
                f"Price per night: INR {session['room_price']:,.2f}\n"
                f"Total: INR {total:,.2f}\n\n"
                "These are demo prices and inventory, not live hotel rates.\n"
                "Reply Yes to create the simulated reservation or No to cancel."
            )
        }

    return None


@app.post("/chat/agent")
def intelligent_passenger_chat(request: ChatRequest):

    booking_id = request.booking_id.strip().upper()
    passenger_id = request.passenger_id.strip().upper()
    message = request.message.strip()

    if not re.fullmatch(r"[A-Z0-9-]{2,30}", booking_id):
        raise HTTPException(status_code=400, detail="Invalid booking reference")

    if not re.fullmatch(r"[A-Z0-9-]{2,30}", passenger_id):
        raise HTTPException(status_code=400, detail="Invalid passenger ID")

    if not message or len(message) > 1000:
        raise HTTPException(status_code=400, detail="Enter a valid message")

    session_key = f"{booking_id}:{passenger_id}"
    normalized = message.lower().strip()

    # STEP 1: Handle explicit passenger confirmation.
    confirmation_words = {
        "yes", "yes please", "confirm", "confirm it",
        "proceed", "proceed please", "submit request",
        "yes submit", "yes proceed"
    }

    cancellation_words = {
        "no", "no thanks", "cancel", "cancel it",
        "don't", "do not proceed", "stop"
    }

    pending = pending_chat_actions.get(session_key)

    # Select which overlapping hotel reservation should be replaced.
    if pending and pending.get("action") == "HOTEL_REPLACEMENT_SELECT":

        if normalized in cancellation_words:
            pending_chat_actions.pop(session_key, None)
            hotel_chat_sessions.pop(session_key, None)

            return {
                "status": "success",
                "intent": "hotel_replacement_cancelled",
                "reply": (
                    "No changes were made. Your existing hotel reservations "
                    "remain confirmed."
                )
            }

        conflicts = pending.get("conflicts", [])
        selected = None

        for index, conflict in enumerate(conflicts, start=1):
            if normalized == str(index):
                selected = conflict
                break

            if normalized.upper() == conflict["booking_ref"].upper():
                selected = conflict
                break

        if not selected:
            return {
                "status": "confirmation_required",
                "intent": "hotel_replacement_selection",
                "reply": (
                    "Please enter a valid reservation number or booking reference "
                    "from the list, or reply No to keep your existing reservations."
                )
            }

        pending_chat_actions[session_key] = {
            "action": "HOTEL_REPLACEMENT_CONFIRMATION",
            "hotel_booking": pending["hotel_booking"],
            "old_booking_ref": selected["booking_ref"]
        }

        new_booking = pending["hotel_booking"]

        return {
            "status": "confirmation_required",
            "intent": "hotel_replacement_confirmation",
            "reply": (
                f"You selected {selected['hotel_name']} "
                f"({selected['booking_ref']}) for replacement.\n\n"
                f"Existing dates: {selected['check_in']} to {selected['check_out']}\n"
                f"New hotel: {new_booking.get('hotel_id')}\n"
                f"New dates: {new_booking['check_in']} to {new_booking['check_out']}\n\n"
                "Do you want to cancel this existing reservation and create "
                "the new reservation?\n"
                "Reply Yes to proceed or No to keep the existing reservation."
            )
        }

    # Confirm or reject replacement of one selected hotel reservation.
    if pending and pending.get("action") == "HOTEL_REPLACEMENT_CONFIRMATION":

        if normalized in confirmation_words:
            try:
                booking_request = HotelBookingRequest(
                    **pending["hotel_booking"]
                )

                old_booking_ref = pending["old_booking_ref"]

                result = replace_hotel_booking(
                    booking_request,
                    old_booking_ref
                )

                pending_chat_actions.pop(session_key, None)
                hotel_chat_sessions.pop(session_key, None)

                return {
                    "status": "success",
                    "intent": "hotel_replacement_confirmed",
                    "hotel_booking": result["booking"],
                    "reply": (
                        "Your hotel reservation has been successfully replaced.\n\n"
                        f"Cancelled reservation: {old_booking_ref}\n"
                        f"New hotel: {result['booking']['hotel_name']}\n"
                        f"New booking reference: {result['booking']['booking_ref']}\n"
                        f"Room: {result['booking']['room_type']}\n"
                        f"Dates: {result['booking']['check_in']} to "
                        f"{result['booking']['check_out']}\n"
                        f"Total: {result['booking']['currency']} "
                        f"{result['booking']['total_amount']:,.2f}\n\n"
                        "Your new simulated reservation is confirmed."
                    )
                }

            except HTTPException as exc:
                pending_chat_actions.pop(session_key, None)
                hotel_chat_sessions.pop(session_key, None)

                return {
                    "status": "error",
                    "intent": "hotel_replacement_error",
                    "reply": str(exc.detail)
                }

            except Exception as exc:
                pending_chat_actions.pop(session_key, None)
                hotel_chat_sessions.pop(session_key, None)

                return {
                    "status": "error",
                    "intent": "hotel_replacement_error",
                    "reply": f"Unable to replace hotel reservation: {exc}"
                }

        if normalized in cancellation_words:
            pending_chat_actions.pop(session_key, None)
            hotel_chat_sessions.pop(session_key, None)

            return {
                "status": "success",
                "intent": "hotel_replacement_cancelled",
                "reply": (
                    "The replacement request has been cancelled. "
                    "Your existing hotel reservation remains confirmed. "
                    "No inventory was changed."
                )
            }

        return {
            "status": "confirmation_required",
            "intent": "hotel_replacement_confirmation",
            "reply": (
                "Please reply Yes to replace the selected hotel reservation "
                "or No to keep your existing reservation."
            )
        }

    # HOTEL BOOKING: independent from flight booking confirmation.
    if pending and pending.get("action") == "HOTEL_BOOKING":

        if normalized in confirmation_words:
            try:
                booking_request = HotelBookingRequest(
                    **pending["hotel_booking"]
                )

                result = book_hotel(booking_request)

                pending_chat_actions.pop(session_key, None)
                hotel_chat_sessions.pop(session_key, None)

                return {
                    "status": "success",
                    "intent": "hotel_booking_confirmed",
                    "hotel_booking": result["booking"],
                    "reply": (
                        "Your SIMULATED hotel reservation has been created.\n\n"
                        f"Hotel: {result['booking']['hotel_name']}\n"
                        f"Booking reference: {result['booking']['booking_ref']}\n"
                        f"Total: {result['booking']['currency']} "
                        f"{result['booking']['total_amount']:,.2f}\n\n"
                        "This is a POC database reservation, not a real hotel booking."
                    )
                }

            except HTTPException as exc:
                pending_chat_actions.pop(session_key, None)
                hotel_chat_sessions.pop(session_key, None)

                return {
                    "status": "error",
                    "intent": "hotel_booking_error",
                    "reply": str(exc.detail)
                }

            except Exception as exc:
                pending_chat_actions.pop(session_key, None)
                hotel_chat_sessions.pop(session_key, None)

                return {
                    "status": "error",
                    "intent": "hotel_booking_error",
                    "reply": f"Unable to create simulated hotel reservation: {exc}"
                }

        if normalized in cancellation_words:
            pending_chat_actions.pop(session_key, None)
            hotel_chat_sessions.pop(session_key, None)

            return {
                "status": "success",
                "intent": "hotel_booking_cancelled",
                "reply": "The pending simulated hotel reservation has been cancelled. No inventory was deducted."
            }

        return {
            "status": "confirmation_required",
            "intent": "hotel_booking_confirmation",
            "reply": "Please reply Yes to confirm the simulated hotel reservation or No to cancel it."
        }

    # Continue hotel conversation before LLM intent classification.
    hotel_response = process_hotel_chat_session(
        session_key, message, booking_id, passenger_id
    )

    if hotel_response is not None:
        return hotel_response

    # Confirm or cancel an existing confirmed passenger booking.
    if pending and pending.get("action") == "CANCEL_CONFIRMED_BOOKING":
        if normalized in confirmation_words:
            try:
                result = cancel_self_service_booking(pending["pnr"])
                pending_chat_actions.pop(session_key, None)
                booking_chat_sessions.pop(session_key, None)
                return {
                    "status": "success",
                    "intent": "booking_cancelled",
                    "cancellation": result,
                    "reply": (
                        f"Your booking {result['pnr']} has been cancelled. "
                        "The seat has been released. You can now ask me "
                        "to find another flight."
                    )
                }
            except HTTPException as exc:
                pending_chat_actions.pop(session_key, None)
                return {
                    "status": "error",
                    "intent": "cancellation_error",
                    "reply": exc.detail
                }

        if normalized in cancellation_words:
            pending_chat_actions.pop(session_key, None)
            return {
                "status": "success",
                "intent": "cancellation_aborted",
                "reply": "Your existing booking has not been cancelled."
            }

        return {
            "status": "confirmation_required",
            "intent": "booking_cancellation_confirmation",
            "reply": "Please reply Yes to cancel the confirmed booking, or No to keep it."
        }

    # SELF-SERVICE BOOKING: final passenger confirmation.
    if pending and pending.get("action") == "SELF_SERVICE_BOOKING":

        if normalized in confirmation_words:
            try:
                booking_data = SelfServiceBooking(
                    booking_id=booking_id,
                    passenger_id=passenger_id,
                    original_flight_id=pending["original_flight_id"],
                    new_flight_id=pending["new_flight_id"],
                    seat_number=pending["seat_number"]
                )

                result = confirm_self_service_booking(booking_data)

                pending_chat_actions.pop(session_key, None)
                booking_chat_sessions.pop(session_key, None)

                return {
                    "status": "success",
                    "intent": "booking_confirmed",
                    "booking": result,
                    "reply": (
                        "Your simulated flight booking is confirmed!\n\n"
                        f"PNR: {result['pnr']}\n"
                        f"Flight: {result['flight_id']}\n"
                        f"Seat: {result['seat_number']}\n"
                        f"Amount: {result['currency']} "
                        f"{result['amount']:,.2f}\n\n"
                        "Your booking reference has been generated."
                    )
                }

            except HTTPException as exc:
                pending_chat_actions.pop(session_key, None)

                return {
                    "status": "error",
                    "intent": "booking_error",
                    "reply": exc.detail
                }

        if normalized in cancellation_words:
            pending_chat_actions.pop(session_key, None)
            booking_chat_sessions.pop(session_key, None)

            return {
                "status": "success",
                "intent": "booking_cancelled",
                "reply": (
                    "No problem. I've cancelled the pending booking "
                    "confirmation. No ticket has been booked."
                )
            }

        return {
            "status": "confirmation_required",
            "intent": "booking_confirmation",
            "reply": "Please reply Yes to confirm the booking or No to cancel it."
        }
    if pending and normalized in confirmation_words:
        if pending["action"] == "REBOOK":
            passenger = pending["passenger"]

            data = RebookingRequest(
                booking_id=booking_id,
                passenger_id=passenger_id,
                original_flight_id=passenger["FLIGHT_ID"],
                action="REBOOK",
                target_flight_id=None,
                hotel_id=None,
                approved_by="passenger-confirmation"
            )

            result = create_request(data)
            pending_chat_actions.pop(session_key, None)

            return {
                "status": "success",
                "intent": "recovery",
                "action_submitted": True,
                "request": result,
                "reply": (
                    "Your rebooking request has been submitted to "
                    "the airline operations team for approval. "
                    "Your ticket has not yet been changed."
                )
            }

    if pending and normalized in cancellation_words:
        pending_chat_actions.pop(session_key, None)

        return {
            "status": "success",
            "intent": "recovery",
            "action_submitted": False,
            "reply": "No problem. I have cancelled the pending rebooking request."
        }
    # Start cancellation of an existing confirmed self-service booking.
    cancellation_request_terms = (
        "cancel my booking",
        "cancel my ticket",
        "cancel my confirmed booking",
        "cancel my confirmed ticket",
        "cancel my flight booking",
        "cancel my rebooking",
        "cancel the booking",
        "cancel the ticket",
        "cancelling my booking",
        "cancelling my ticket",
        "cancelling the booking",
        "cancelling the ticket",
        "help me cancel",
        "help me in cancelling",
        "want to cancel",
        "i want cancellation",
        "request cancellation",
        "cancel my flight",
        "cancel booking",
        "cancel ticket"
    )

    if any(term in normalized for term in cancellation_request_terms):
        conn = booking_db()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT pnr, new_flight_id, seat_number
                FROM passenger_bookings
                WHERE booking_id = %s
                  AND passenger_id = %s
                  AND booking_status = 'CONFIRMED'
                ORDER BY created_at DESC
                LIMIT 1
            """, (booking_id, passenger_id))
            active_booking = cur.fetchone()
        finally:
            conn.close()

        if not active_booking:
            return {
                "status": "success",
                "intent": "no_active_booking",
                "reply": (
                    "I couldn't find an active confirmed self-service "
                    "booking for this booking reference and passenger."
                )
            }

        pending_chat_actions[session_key] = {
            "action": "CANCEL_CONFIRMED_BOOKING",
            "pnr": active_booking["pnr"]
        }

        return {
            "status": "confirmation_required",
            "intent": "booking_cancellation_confirmation",
            "reply": (
                f"I found your confirmed booking {active_booking['pnr']} "
                f"on flight {active_booking['new_flight_id']}, "
                f"seat {active_booking['seat_number']}.\n\n"
                "Are you sure you want to cancel it? "
                "Reply Yes to cancel or No to keep the booking."
            )
        }

    # CONVERSATIONAL FLIGHT AND SEAT SELECTION
    booking_session = booking_chat_sessions.get(session_key)

    # Passenger selected a flight.
    if booking_session and booking_session.get("stage") == "SELECT_FLIGHT":
        message_upper = message.strip().upper()

        # First try matching the complete flight ID.
        matching_flight = next(
            (
                flight for flight in booking_session["flights"]
                if flight["flight_id"].upper() in message_upper
            ),
            None
        )

        # Also support a short flight number if it is unique.
        if not matching_flight:
            short_matches = [
                flight for flight in booking_session["flights"]
                if flight["flight_id"].split("-")[0].upper() == message_upper
            ]

            if len(short_matches) == 1:
                matching_flight = short_matches[0]

        if not matching_flight:
            return {
                "status": "success",
                "intent": "flight_selection",
                "reply": (
                    "I couldn't find that flight in the alternatives "
                    "I showed. Please enter one of the displayed "
                    "flight IDs."
                )
            }

        selected_flight_id = matching_flight["flight_id"]

        try:
            seat_result = get_booking_seats(selected_flight_id)

            available_seats = [
                seat for seat in seat_result["seats"]
                if seat["seat_status"] == "AVAILABLE"
            ]

            if not available_seats:
                return {
                    "status": "success",
                    "intent": "seat_selection",
                    "reply": (
                        "Unfortunately, this flight has no available "
                        "seats. Please select another alternative."
                    )
                }

            booking_session["stage"] = "SELECT_SEAT"
            booking_session["selected_flight"] = matching_flight
            booking_session["seats"] = available_seats

            return {
                "status": "success",
                "intent": "seat_selection",
                "flight": matching_flight,
                "seats": available_seats,
                "reply": (
                    f"Great! I've selected {matching_flight['airline']} "
                    f"{selected_flight_id}.\n\n"
                    f"Fare: {matching_flight['currency']} "
                    f"{matching_flight['price']}\n\n"
                    "Please select your preferred available seat."
                )
            }

        except HTTPException as exc:
            return {
                "status": "error",
                "intent": "seat_error",
                "reply": str(exc.detail)
            }

    # Passenger selected a seat.
    if booking_session and booking_session.get("stage") == "SELECT_SEAT":
        seat_match = re.search(
            r"\b([1-6][A-F])\b",
            message.upper()
        )

        if seat_match:
            selected_seat = seat_match.group(1)

            available = {
                seat["seat_number"]
                for seat in booking_session["seats"]
            }

            if selected_seat not in available:
                return {
                    "status": "success",
                    "intent": "seat_selection",
                    "reply": (
                        f"Seat {selected_seat} is not currently available. "
                        "Please select another available seat."
                    ),
                    "seats": booking_session["seats"]
                }

            flight = booking_session["selected_flight"]

            booking_session["stage"] = "CONFIRM_BOOKING"
            booking_session["selected_seat"] = selected_seat

            pending_chat_actions[session_key] = {
                "action": "SELF_SERVICE_BOOKING",
                "original_flight_id": booking_session["original_flight_id"],
                "new_flight_id": flight["flight_id"],
                "seat_number": selected_seat
            }

            return {
                "status": "confirmation_required",
                "intent": "booking_confirmation",
                "flight": flight,
                "seat_number": selected_seat,
                "reply": (
                    f"You selected seat {selected_seat} on "
                    f"{flight['airline']} {flight['flight_id']}.\n\n"
                    f"Total fare: {flight['currency']} "
                    f"{float(flight['price']):,.2f}.\n\n"
                    "Would you like me to confirm this booking? "
                    "Please reply Yes or No."
                )
            }

    # Smart passenger booking information lookup.
    booking_detail_terms = (
        "booking details", "current booking", "my booking",
        "reservation details", "show my booking", "view my booking",
        "show booking", "show my ticket", "my reservation"
    )

    pnr_terms = (
        "what is my pnr", "what's my pnr", "my pnr",
        "tell me my pnr", "find my pnr", "get my pnr",
        "booking reference", "pnr number"
    )

    status_terms = (
        "is my ticket cancelled", "is my ticket canceled",
        "has my ticket been cancelled", "has my ticket been canceled",
        "is my booking cancelled", "is my booking canceled",
        "is my booking confirmed", "booking status",
        "ticket status", "is my ticket confirmed",
        "status of booking", "status of my booking",
        "current status of booking", "current status of my booking",
        "status of my ticket", "current status of my ticket",
        "what is my booking status", "what is my ticket status"
    )

    flight_terms = (
        "which flight did i book", "which flight am i booked",
        "what flight am i on", "which flight is my booking",
        "my flight number", "what is my flight"
    )

    seat_terms = (
        "what is my seat", "what is my seat number",
        "which seat am i assigned", "my seat number",
        "which seat did i book"
    )

    amount_terms = (
        "how much did i pay", "how much is my ticket",
        "ticket amount", "booking amount", "fare amount"
    )

    is_booking_question = any(
        term in normalized
        for group in (
            booking_detail_terms, pnr_terms, status_terms,
            flight_terms, seat_terms, amount_terms
        )
        for term in group
    )

    if is_booking_question:
        conn = booking_db()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT pnr, booking_id, passenger_id,
                       original_flight_id, new_flight_id,
                       seat_number, amount, currency,
                       booking_status, created_at
                FROM passenger_bookings
                WHERE booking_id = %s
                  AND passenger_id = %s
                ORDER BY created_at DESC
                LIMIT 1
            """, (booking_id, passenger_id))
            booking_details = cur.fetchone()
        finally:
            conn.close()

        if not booking_details:
            # Fallback: retrieve itinerary from Flink/MCP.
            itinerary_sql = (
                'SELECT "PASSENGER_ID", "BOOKING_ID", "FLIGHT_ID", '
                '"NEXT_FLIGHT_ID", "CONNECTION_AIRPORT", '
                '"FLIGHT_STATUS", "DELAY_MINUTES", '
                '"REMAINING_CONNECTION_MINUTES", "CONNECTION_RISK" '
                'FROM passenger_state_stream '
                f'WHERE "BOOKING_ID" = \'{booking_id}\' '
                f'AND "PASSENGER_ID" = \'{passenger_id}\' LIMIT 5'
            )

            try:
                itineraries = mcp_query(
                    "passenger_state_stream",
                    itinerary_sql
                )
            except Exception as exc:
                itineraries = []
                print(f"Booking itinerary fallback failed: {exc}")

            if itineraries:
                item = itineraries[0]

                reply = (
                    "I found your passenger itinerary, but I could not "
                    "find a ticket record in the booking database.\n\n"
                    f"Booking ID: {item.get('BOOKING_ID', booking_id)}\n"
                    f"Passenger ID: {item.get('PASSENGER_ID', passenger_id)}\n"
                    f"Flight: {item.get('FLIGHT_ID', 'Not available')}\n"
                    f"Connecting flight: {item.get('NEXT_FLIGHT_ID') or 'Not available'}\n"
                    f"Connection airport: {item.get('CONNECTION_AIRPORT') or 'Not available'}\n"
                    f"Flight status: {item.get('FLIGHT_STATUS') or 'Not available'}\n"
                    f"Delay: {item.get('DELAY_MINUTES') if item.get('DELAY_MINUTES') is not None else 'Not available'} minutes\n"
                    f"Remaining connection time: "
                    f"{item.get('REMAINING_CONNECTION_MINUTES') if item.get('REMAINING_CONNECTION_MINUTES') is not None else 'Not available'} minutes\n"
                    f"Connection risk: {item.get('CONNECTION_RISK') or 'Not available'}"
                )

                return {
                    "status": "success",
                    "intent": "booking_itinerary",
                    "reply": reply,
                    "context": {"passenger": item}
                }

            return {
                "status": "success",
                "intent": "booking_details",
                "reply": (
                    "I could not find a ticket booking or passenger itinerary "
                    f"for booking {booking_id} and passenger {passenger_id}."
                )
            }

        b = booking_details
        status = str(b["booking_status"]).upper()
        pnr = b["pnr"]
        flight = b["new_flight_id"]
        seat = b["seat_number"]
        amount = f'{b["currency"]} {float(b["amount"]):,.2f}'

        if any(term in normalized for term in pnr_terms):
            reply = f"Your PNR is {pnr}."

        elif any(term in normalized for term in status_terms):
            if status == "CANCELLED":
                reply = f"Yes, your ticket (PNR {pnr}) is cancelled. The seat has been released."
            elif status == "CONFIRMED":
                reply = f"Your ticket (PNR {pnr}) is confirmed."
            else:
                reply = f"Your booking status is {status} (PNR {pnr})."

        elif any(term in normalized for term in flight_terms):
            reply = f"Your booked flight is {flight}. PNR: {pnr}."

        elif any(term in normalized for term in seat_terms):
            reply = f"Your assigned seat is {seat}. PNR: {pnr}."

        elif any(term in normalized for term in amount_terms):
            reply = f"Your ticket amount is {amount}. PNR: {pnr}."

        else:
            reply = (
                f"Here are your latest booking details:\n\n"
                f"PNR: {pnr}\n"
                f"Flight: {flight}\n"
                f"Seat: {seat}\n"
                f"Amount: {amount}\n"
                f"Booking status: {status}"
            )
            if status == "CANCELLED":
                reply += "\n\nYour booking has been cancelled and the seat has been released."
            elif status == "CONFIRMED":
                reply += "\n\nYour booking is confirmed."

        return {
            "status": "success",
            "intent": "booking_information",
            "reply": reply
        }

    model = get_granite_model()

    alternative_terms = (
        "alternative flight", "alternative flights",
        "other flight", "other flights",
        "available flights", "show flights",
        "find flights", "flight options",
        "different flight"
    )

    if any(term in normalized for term in alternative_terms):
        intent = "alternative_flights"
    else:
        intent = classify_passenger_intent(model, message)

    # REBOOKING STATUS: answer from the latest Kafka event.
    status_terms = (
        "status", "update", "approved", "rejected",
        "pending", "what happened", "request progress"
    )
    rebooking_terms = (
        "rebook", "re-book", "reschedule",
        "booking request", "rebooking request"
    )

    if (
        any(term in normalized for term in status_terms)
        and any(term in normalized for term in rebooking_terms)
    ):
        events = read_kafka_topic(
            topic=TOPIC,
            key_format="avro",
            latest_by_key=True,
            limit=50000
        )

        matching = [
            event for event in events
            if event.get("booking_id") == booking_id
            and event.get("passenger_id") == passenger_id
            and event.get("action") in ("REBOOK", "BOTH")
        ]

        latest = max(
            matching,
            key=lambda event: int(event.get("event_ts") or 0),
            default=None
        )

        if not latest:
            reply = (
                "I couldn't find a rebooking request for your booking yet. "
                "Would you like assistance submitting one?"
            )
            status = "NO_REQUEST_FOUND"
        else:
            status = latest.get("status", "UNKNOWN")
            flight_id = latest.get("original_flight_id", "your flight")

            if status == "PENDING_APPROVAL":
                reply = (
                    f"Your rebooking request for flight {flight_id} "
                    "is currently pending approval with the airline "
                    "operations team. Your ticket has not yet been changed."
                )
            elif status == "APPROVED":
                reply = (
                    f"Your rebooking request for flight {flight_id} "
                    "has been approved by the airline operations team. "
                    "Please note that approval alone does not confirm "
                    "that a new ticket has been issued."
                )
            elif status == "REJECTED":
                reply = (
                    f"Your rebooking request for flight {flight_id} "
                    "was rejected by the airline operations team. "
                    "No ticket change has been made."
                )
            else:
                reply = f"Your latest rebooking request status is {status}."

        return {
            "status": "success",
            "intent": "rebooking_status",
            "request_status": status,
            "reply": reply
        }

    if intent == "general":
        reply = generate_agent_response(model, message, intent, {})
        return {
            "status": "success",
            "intent": intent,
            "reply": reply,
            "context": {}
        }

    # Retrieve passenger context.
    passenger_sql = (
        'SELECT "PASSENGER_ID", "BOOKING_ID", "FLIGHT_ID", '
        '"NEXT_FLIGHT_ID", "CONNECTION_AIRPORT", "FLIGHT_STATUS", '
        '"DELAY_MINUTES", "REMAINING_CONNECTION_MINUTES", "CONNECTION_RISK" '
        'FROM passenger_state_stream '
        f'WHERE "BOOKING_ID" = \'{booking_id}\' '
        f'AND "PASSENGER_ID" = \'{passenger_id}\' LIMIT 5'
    )

    passengers = mcp_query("passenger_state_stream", passenger_sql)

    if not passengers:
        raise HTTPException(
            status_code=404,
            detail="No matching passenger booking found"
        )

    passenger = passengers[0]
    context = {"passenger": passenger}

    if intent in {"flight", "recovery", "alternative_flights"}:

        # Always prefer the passenger's latest active confirmed ticket.
        conn = booking_db()
        try:
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT
                    pb.new_flight_id,
                    fi.origin,
                    fi.destination
                FROM passenger_bookings pb
                LEFT JOIN flight_inventory fi
                  ON fi.flight_id = pb.new_flight_id
                WHERE pb.booking_id = %s
                  AND pb.passenger_id = %s
                  AND pb.booking_status = 'CONFIRMED'
                ORDER BY pb.created_at DESC
                LIMIT 1
            """, (booking_id, passenger_id))
            active_booking = cur.fetchone()
        finally:
            conn.close()

        active_origin = None
        active_destination = None

        if active_booking:
            flight_id = active_booking["new_flight_id"]
            active_origin = active_booking.get("origin")
            active_destination = active_booking.get("destination")

        elif intent == "flight":
            return {
                "status": "success",
                "intent": "flight_status",
                "reply": (
                    "You currently have no active confirmed flight ticket. "
                    "Your previous ticket may have been cancelled. "
                    "Would you like to see alternative flights?"
                )
            }

        else:
            # Preserve the original itinerary for alternative-flight
            # discovery when the passenger has no active ticket.
            flight_id = passenger["FLIGHT_ID"]

        flight_sql = (
            'SELECT "FLIGHT_ID", "AIRLINE", "ORIGIN", "DESTINATION", '
            '"STATUS", "DELAY_MINUTES", "SCHEDULED_ARRIVAL", '
            '"ESTIMATED_ARRIVAL", "GATE", "DELAY_REASON" '
            'FROM flight_status_v2 '
            f'WHERE "FLIGHT_ID" = \'{flight_id}\' LIMIT 5'
        )

        flights = mcp_query("flight_status_v2", flight_sql)
        context["flight"] = flights[0] if flights else None

    if intent == "alternative_flights":
        original_flight = context.get("flight") or {}

        # Prefer the actual route from the latest confirmed booking.
        # Fall back to flight-status data only when needed.
        origin = (
            active_origin
            or original_flight.get("ORIGIN")
            or original_flight.get("origin")
        )
        destination = (
            active_destination
            or original_flight.get("DESTINATION")
            or original_flight.get("destination")
        )

        if not origin or not destination:
            return {
                "status": "success",
                "intent": intent,
                "reply": (
                    "I couldn't determine the route from your active "
                    "booking. Please contact airline support for help."
                )
            }

        origin = str(origin).strip().upper()
        destination = str(destination).strip().upper()

        alternatives = get_alternative_flights(
            origin=origin,
            destination=destination,
            min_seats=1,
            limit=20
        )

        context["alternative_flights"] = alternatives["data"]

        if not alternatives["data"]:
            return {
                "status": "success",
                "intent": intent,
                "reply": (
                    f"I couldn't find any future {origin} to {destination} "
                    "flights with available seats at the moment."
                )
            }

        booking_chat_sessions[session_key] = {
            "stage": "SELECT_FLIGHT",
            "original_flight_id": passenger["FLIGHT_ID"],
            "flights": alternatives["data"]
        }

        flight_lines = []

        for index, flight in enumerate(alternatives["data"][:10], 1):
            flight_lines.append(
                f"{index}. {flight['airline']} - {flight['flight_id']}\n"
                f"   {flight['origin']} to {flight['destination']}\n"
                f"   Departure: {flight['departure_time']}\n"
                f"   Available seats: {flight['available_seats']}\n"
                f"   Fare: {flight['currency']} "
                f"{float(flight['price']):,.2f}"
            )

        return {
            "status": "success",
            "intent": "alternative_flights",
            "flights": alternatives["data"][:10],
            "reply": (
                f"I found {len(alternatives['data'])} alternative flights "
                f"from {origin} to {destination}.\n\n"
                + "\n\n".join(flight_lines)
                + "\n\nWhich flight would you like to select? "
                  "You can enter its flight number."
            )
        }
    hotel_terms = (
        "hotel", "hotels", "accommodation", "accommodations",
        "stay", "place to stay", "guest house", "hostel"
    )

    wants_hotels = (
        intent == "hotel"
        or (
            intent == "recovery"
            and any(term in normalized for term in hotel_terms)
        )
    )

    if wants_hotels:
        airport_code = get_passenger_airport(
            booking_id, passenger_id
        )

        if not airport_code:
            raise HTTPException(
                status_code=400,
                detail="Unable to determine the origin airport from your confirmed booking."
            )

        hotel_result = discover_osm_hotels(
            airport_code=airport_code,
            radius_km=15,
            limit=30
        )

        context["hotels"] = hotel_result["data"]
        context["hotel_discovery"] = {
            "airport_code": hotel_result["airport_code"],
            "airport_name": hotel_result["airport_name"],
            "source": hotel_result["inventory_source"],
            "disclaimer": hotel_result["disclaimer"],
            "attribution":  hotel_result.get("attribution", "Hotel inventory fallback"),
            "attribution_url": hotel_result.get("attribution_url", "https://www.openstreetmap.org/copyright")
        }

        if intent == "hotel":
            if context["hotels"]:
                hotel_chat_sessions[session_key] = {
                    "stage": "SELECT_HOTEL",
                    "hotels": context["hotels"],
                    "airport_code": hotel_result["airport_code"],
                    "city": hotel_result["city"]
                }

            if context["hotels"]:
                hotel_lines = [
                    f"{index}. {hotel['name']} "
                    f"({hotel['tourism'].replace('_', ' ')}) - "
                    f"{hotel['distance_km']} km from {hotel_result['airport_code']}"
                    for index, hotel in enumerate(context["hotels"][:10], 1)
                ]

                reply = (
                    f"I found {len(context['hotels'])} mapped accommodation "
                    f"listings within 15 km of {hotel_result['airport_name']} "
                    f"({hotel_result['airport_code']}).\n\n"
                    + "\n".join(hotel_lines)
                    + "\n\nThese are mapped listings, not confirmed room "
                    "availability or prices. You can open each listing's "
                    "website or map directions below."
                )
            else:
                reply = (
                    f"I couldn't find named accommodation listings within "
                    f"15 km of {hotel_result['airport_name']}. "
                    "You can try a larger search radius."
                )

            return {
                "status": "success",
                "intent": "hotel",
                "reply": reply,
                "hotels": context["hotels"],
                "hotel_discovery": context["hotel_discovery"]
            }

    # Only create a pending action for an explicit rebooking request.
    rebooking_terms = (
        "rebook", "re-book", "reschedule", "change my flight",
        "change flight", "book another flight"
    )

    if intent == "recovery" and any(term in normalized for term in rebooking_terms):

        pending_chat_actions[session_key] = {
            "action": "REBOOK",
            "passenger": passenger
        }

        return {
            "status": "confirmation_required",
            "intent": intent,
            "action": "REBOOK",
            "reply": (
                "I can submit a rebooking request for your flight "
                f"{passenger['FLIGHT_ID']}. Would you like me to submit "
                "this request to the airline operations team for approval? "
                "Please reply Yes to confirm or No to cancel. "
                "Your ticket will not be changed until operations approves it."
            ),
            "context": context
        }

    reply = generate_agent_response(model, message, intent, context)

    return {
        "status": "success",
        "intent": intent,
        "reply": reply,
        "context": context
    }


# ============================================================
# AIRLINEIQ SELF-SERVICE FLIGHT BOOKING
# ============================================================

def booking_db():
    return psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="airline",
        user="postgres",
        password=os.getenv("AIRLINE_PG_PASSWORD", "change-me")
    )



def get_confirmed_rebooked_flight(booking_id, passenger_id):
    """Return the latest confirmed rebooked flight, if one exists."""
    conn = booking_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT new_flight_id
            FROM passenger_bookings
            WHERE booking_id = %s
              AND passenger_id = %s
              AND booking_status = 'CONFIRMED'
              AND new_flight_id IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
        """, (booking_id, passenger_id))

        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def load_booking_flight(flight_id):
    """Load selected flight from Kafka into PostgreSQL inventory."""

    conn = booking_db()

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            "SELECT * FROM flight_inventory WHERE flight_id = %s",
            (flight_id,)
        )
        flight = cur.fetchone()

        if flight:
            return dict(flight)

        records = read_kafka_topic(
            topic="alternative_flights",
            key_format="string",
            latest_by_key=False,
            limit=50000
        )

        selected = next(
            (r for r in records if r.get("flight_id") == flight_id),
            None
        )

        if not selected:
            raise HTTPException(status_code=404, detail="Flight not found")

        cur.execute("""
            INSERT INTO flight_inventory
            (flight_id, airline, origin, destination,
             departure_time, arrival_time, status, price, currency)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (flight_id) DO UPDATE SET
                airline = EXCLUDED.airline,
                departure_time = EXCLUDED.departure_time,
                arrival_time = EXCLUDED.arrival_time,
                status = EXCLUDED.status,
                price = EXCLUDED.price,
                updated_at = NOW()
            RETURNING *
        """, (
            selected["flight_id"],
            selected["airline"],
            selected["origin"],
            selected["destination"],
            selected["departure_time"],
            selected.get("arrival_time"),
            selected["status"],
            selected["price"],
            selected.get("currency", "INR")
        ))

        flight = dict(cur.fetchone())

        # Six rows, six seats per row: 36 simulated economy seats.
        # Seed availability based on simulator's available_seats count.
        available = max(
            0, min(36, int(selected.get("available_seats", 0)))
        )

        for row in range(1, 7):
            for letter in "ABCDEF":
                seat = f"{row}{letter}"
                seat_number = (row - 1) * 6 + ord(letter) - ord("A")

                seat_status = (
                    "AVAILABLE" if seat_number >= 36 - available
                    else "BOOKED"
                )

                cur.execute("""
                    INSERT INTO flight_seats
                    (flight_id, seat_number, seat_class, seat_status)
                    VALUES (%s,%s,'ECONOMY',%s)
                    ON CONFLICT (flight_id, seat_number) DO NOTHING
                """, (flight_id, seat, seat_status))

        conn.commit()
        return flight

    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()


@app.get("/booking/seats/{flight_id}")
def get_booking_seats(flight_id: str):

    flight = load_booking_flight(flight_id)

    conn = booking_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT seat_number, seat_class, seat_status
            FROM flight_seats
            WHERE flight_id = %s
            ORDER BY LENGTH(seat_number), seat_number
        """, (flight_id,))

        seats = [dict(r) for r in cur.fetchall()]

        return {
            "flight": flight,
            "seats": seats,
            "available_count": sum(
                1 for s in seats if s["seat_status"] == "AVAILABLE"
            )
        }
    finally:
        conn.close()


class SelfServiceBooking(BaseModel):
    booking_id: str
    passenger_id: str
    original_flight_id: str
    new_flight_id: str
    seat_number: str


@app.post("/booking/confirm")
def confirm_self_service_booking(data: SelfServiceBooking):

    conn = booking_db()

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # A passenger may have only one active self-service booking.
        cur.execute("""
            SELECT pnr, new_flight_id, seat_number
            FROM passenger_bookings
            WHERE booking_id = %s
              AND passenger_id = %s
              AND booking_status = 'CONFIRMED'
            LIMIT 1
            FOR UPDATE
        """, (data.booking_id, data.passenger_id))

        active_booking = cur.fetchone()
        if active_booking:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"You already have an active booking (PNR "
                    f"{active_booking['pnr']}, flight "
                    f"{active_booking['new_flight_id']}, seat "
                    f"{active_booking['seat_number']}). "
                    "Cancel that booking before booking another flight."
                )
            )

        # Lock and reserve only if the seat is still available.
        cur.execute("""
            UPDATE flight_seats
            SET seat_status = 'RESERVED', updated_at = NOW()
            WHERE flight_id = %s
              AND seat_number = %s
              AND seat_status = 'AVAILABLE'
            RETURNING seat_number
        """, (data.new_flight_id, data.seat_number))

        reserved = cur.fetchone()

        if not reserved:
            conn.rollback()
            raise HTTPException(
                status_code=409,
                detail="This seat is no longer available. Please select another seat."
            )

        cur.execute("""
            SELECT price, currency, status
            FROM flight_inventory
            WHERE flight_id = %s
            FOR UPDATE
        """, (data.new_flight_id,))

        flight = cur.fetchone()

        if not flight:
            conn.rollback()
            raise HTTPException(status_code=404, detail="Flight not found")

        if flight["status"] not in ("SCHEDULED", "DELAYED"):
            conn.rollback()
            raise HTTPException(
                status_code=400,
                detail="This flight is not available for booking"
            )

        pnr = uuid.uuid4().hex[:8].upper()

        cur.execute("""
            INSERT INTO passenger_bookings
            (pnr, booking_id, passenger_id, original_flight_id,
             new_flight_id, seat_number, amount, currency, booking_status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'CONFIRMED')
        """, (
            pnr,
            data.booking_id,
            data.passenger_id,
            data.original_flight_id,
            data.new_flight_id,
            data.seat_number,
            flight["price"],
            flight["currency"]
        ))

        cur.execute("""
            UPDATE flight_seats
            SET seat_status = 'BOOKED', booking_id = %s, updated_at = NOW()
            WHERE flight_id = %s AND seat_number = %s
        """, (pnr, data.new_flight_id, data.seat_number))

        conn.commit()

        return {
            "status": "CONFIRMED",
            "message": "Your simulated self-service booking is confirmed.",
            "pnr": pnr,
            "booking_id": data.booking_id,
            "passenger_id": data.passenger_id,
            "flight_id": data.new_flight_id,
            "seat_number": data.seat_number,
            "amount": float(flight["price"]),
            "currency": flight["currency"]
        }

    except HTTPException:
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()


@app.post("/booking/cancel/{pnr}")
def cancel_self_service_booking(pnr: str):

    conn = booking_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT pnr, new_flight_id, seat_number, booking_status
            FROM passenger_bookings
            WHERE pnr = %s
            FOR UPDATE
        """, (pnr.upper(),))

        booking = cur.fetchone()

        if not booking:
            raise HTTPException(status_code=404, detail="PNR not found")

        if booking["booking_status"] == "CANCELLED":
            raise HTTPException(status_code=409, detail="This booking is already cancelled")

        if booking["booking_status"] != "CONFIRMED":
            raise HTTPException(status_code=409, detail="Only confirmed bookings can be cancelled")

        cur.execute("""
            UPDATE passenger_bookings
            SET booking_status = 'CANCELLED'
            WHERE pnr = %s
        """, (pnr.upper(),))

        cur.execute("""
            UPDATE flight_seats
            SET seat_status = 'AVAILABLE',
                booking_id = NULL,
                updated_at = NOW()
            WHERE flight_id = %s
              AND seat_number = %s
              AND booking_id = %s
        """, (booking["new_flight_id"], booking["seat_number"], pnr.upper()))

        conn.commit()

        return {
            "status": "CANCELLED",
            "pnr": pnr.upper(),
            "message": "Booking cancelled successfully. The seat has been released. You can book another flight now."
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()


@app.get("/booking/details/{pnr}")
def get_booking_details(pnr: str):

    conn = booking_db()
    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT b.*, f.airline, f.origin, f.destination,
                   f.departure_time, f.arrival_time
            FROM passenger_bookings b
            JOIN flight_inventory f
              ON b.new_flight_id = f.flight_id
            WHERE b.pnr = %s
        """, (pnr.upper(),))

        result = cur.fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="PNR not found")

        result = dict(result)

        for key in ("amount",):
            result[key] = float(result[key])

        return result

    finally:
        conn.close()

# ============================================================
# AIRLINEIQ - HOTEL DISCOVERY AND SELF-SERVICE BOOKING
# ============================================================

import math as hotel_math
from datetime import date as hotel_date
from datetime import datetime as hotel_datetime
from datetime import timezone as hotel_timezone
from pydantic import BaseModel as HotelBaseModel, Field as HotelField


HOTEL_AIRPORTS = {
    "BLR": {
        "name": "Kempegowda International Airport",
        "city": "Bengaluru",
        "latitude": 13.1986,
        "longitude": 77.7066
    },
    "BOM": {
        "name": "Chhatrapati Shivaji Maharaj International Airport",
        "city": "Mumbai",
        "latitude": 19.0896,
        "longitude": 72.8656
    },
    "DEL": {
        "name": "Indira Gandhi International Airport",
        "city": "Delhi",
        "latitude": 28.5562,
        "longitude": 77.1000
    }
}


def hotel_distance_km(lat1, lon1, lat2, lon2):
    """Calculate distance between airport and hotel using Haversine."""
    radius = 6371.0

    lat1, lon1, lat2, lon2 = map(
        hotel_math.radians, [lat1, lon1, lat2, lon2]
    )

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        hotel_math.sin(dlat / 2) ** 2
        + hotel_math.cos(lat1)
        * hotel_math.cos(lat2)
        * hotel_math.sin(dlon / 2) ** 2
    )

    return round(
        radius * 2 * hotel_math.atan2(
            hotel_math.sqrt(a),
            hotel_math.sqrt(1 - a)
        ),
        2
    )


def initialize_hotel_tables():
    conn = booking_db()

    try:
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS hotel_inventory (
                hotel_id VARCHAR(50) PRIMARY KEY,
                hotel_name VARCHAR(200) NOT NULL,
                city VARCHAR(100) NOT NULL,
                airport_code VARCHAR(10) NOT NULL,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                available_rooms INTEGER NOT NULL DEFAULT 0,
                price_per_night NUMERIC(12,2) NOT NULL,
                currency VARCHAR(10) NOT NULL DEFAULT 'INR',
                room_types JSONB NOT NULL DEFAULT '[]'::jsonb,
                amenities JSONB NOT NULL DEFAULT '[]'::jsonb,
                rating NUMERIC(2,1) DEFAULT 0,
                image_url TEXT,
                inventory_source VARCHAR(30) DEFAULT 'SIMULATED',
                updated_at TIMESTAMP DEFAULT NOW(),
                CHECK (available_rooms >= 0)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS hotel_bookings (
                booking_ref VARCHAR(30) PRIMARY KEY,
                booking_id VARCHAR(100) NOT NULL,
                passenger_id VARCHAR(100) NOT NULL,
                hotel_id VARCHAR(50) NOT NULL
                    REFERENCES hotel_inventory(hotel_id),
                hotel_name VARCHAR(200) NOT NULL,
                room_type VARCHAR(100) NOT NULL,
                check_in DATE NOT NULL,
                check_out DATE NOT NULL,
                nights INTEGER NOT NULL,
                guests INTEGER NOT NULL DEFAULT 1,
                rooms INTEGER NOT NULL DEFAULT 1,
                price_per_night NUMERIC(12,2) NOT NULL,
                total_amount NUMERIC(12,2) NOT NULL,
                currency VARCHAR(10) NOT NULL DEFAULT 'INR',
                booking_status VARCHAR(20) NOT NULL DEFAULT 'CONFIRMED',
                created_at TIMESTAMP DEFAULT NOW(),
                CHECK (check_out > check_in),
                CHECK (nights > 0),
                CHECK (rooms > 0)
            )
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_hotel_inventory_airport
            ON hotel_inventory(airport_code)
        """)

        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_hotel_bookings_passenger
            ON hotel_bookings(passenger_id, created_at DESC)
        """)

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class HotelBookingRequest(HotelBaseModel):
    booking_id: str
    passenger_id: str
    hotel_id: str
    room_type: str
    check_in: hotel_date
    check_out: hotel_date
    guests: int = HotelField(default=1, ge=1, le=10)
    rooms: int = HotelField(default=1, ge=1, le=5)


def get_passenger_airport(booking_id, passenger_id):
    """
    Resolve the passenger's latest confirmed flight origin.
    Prefer the rebooked flight when one exists.
    """
    conn = booking_db()

    try:
        cur = conn.cursor()

        cur.execute("""
            SELECT fi.origin
            FROM passenger_bookings pb
            JOIN flight_inventory fi
              ON fi.flight_id = pb.new_flight_id
            WHERE pb.booking_id = %s
              AND pb.passenger_id = %s
              AND pb.booking_status = 'CONFIRMED'
            ORDER BY pb.created_at DESC
            LIMIT 1
        """, (booking_id, passenger_id))

        row = cur.fetchone()

        if row and row[0]:
            return row[0].upper()

        # Fallback to the passenger's original itinerary in Kafka.
        records = read_kafka_topic(
            "passenger_itineraries",
            key_format="string",
            latest_by_key=False,
            limit=50000
        )

        passenger = next(
            (
                item for item in records
                if str(item.get("passenger_id", "")).upper()
                == str(passenger_id).upper()
            ),
            None
        )

        if passenger:
            origin = (
                passenger.get("origin")
                or passenger.get("ORIGIN")
                or passenger.get("departure_airport")
            )
            if origin:
                return str(origin).upper()

        return None

    finally:
        conn.close()





def ensure_osm_demo_inventory(hotel, airport_code):
    """
    Create persistent simulated inventory for an OSM hotel.
    OSM supplies the hotel identity and location; all prices and
    room availability generated here are DEMO data.
    """
    import random

    airport_code = airport_code.upper()
    airport = HOTEL_AIRPORTS.get(airport_code)

    if not airport:
        raise HTTPException(
            status_code=400,
            detail="Unsupported airport for hotel inventory."
        )

    osm_type = str(hotel.get("osm_type", "node")).upper()
    osm_id = str(hotel.get("osm_id", ""))

    if not osm_id.isdigit():
        raise HTTPException(
            status_code=400,
            detail="The selected hotel has an invalid OpenStreetMap ID."
        )

    hotel_id = f"OSM-{osm_type}-{osm_id}"[:50]

    rng = random.Random(f"{osm_type}-{osm_id}")

    base_price = rng.randrange(2500, 6501, 250)

    room_types = [
        {
            "name": "Standard Room",
            "price": base_price,
            "available_rooms": rng.randint(5, 15)
        },
        {
            "name": "Deluxe Room",
            "price": base_price + rng.randrange(1000, 2001, 250),
            "available_rooms": rng.randint(3, 10)
        },
        {
            "name": "Executive Suite",
            "price": base_price + rng.randrange(3000, 4501, 250),
            "available_rooms": rng.randint(1, 5)
        }
    ]

    total_rooms = sum(
        room["available_rooms"] for room in room_types
    )

    conn = booking_db()

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            INSERT INTO hotel_inventory (
                hotel_id, hotel_name, city, airport_code,
                latitude, longitude, available_rooms,
                price_per_night, currency, room_types,
                amenities, inventory_source
            )
            VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,'INR',
                %s::jsonb,'[]'::jsonb,'DEMO_OSM'
            )
            ON CONFLICT (hotel_id) DO NOTHING
        """, (
            hotel_id,
            str(hotel["name"])[:200],
            airport["city"],
            airport_code,
            float(hotel["latitude"]),
            float(hotel["longitude"]),
            total_rooms,
            base_price,
            json.dumps(room_types)
        ))

        cur.execute("""
            SELECT hotel_id, hotel_name, available_rooms,
                   price_per_night, currency, room_types,
                   inventory_source
            FROM hotel_inventory
            WHERE hotel_id = %s
        """, (hotel_id,))

        result = cur.fetchone()
        conn.commit()

        if not result:
            raise HTTPException(
                status_code=500,
                detail="Unable to create hotel demo inventory."
            )

        return dict(result)

    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()


# ============================================================
# REAL HOTEL DISCOVERY - OPENSTREETMAP / OVERPASS
# No API key or paid subscription required.
# ============================================================

OSM_HOTEL_CACHE = {}
OSM_HOTEL_CACHE_TTL = 3600

def discover_osm_hotels(airport_code, radius_km=15, limit=30):
    import json
    import math
    import urllib.parse
    import urllib.request

    airport_code = airport_code.upper()
    airport = HOTEL_AIRPORTS.get(airport_code)

    if not airport:
        raise HTTPException(
            status_code=400,
            detail=f"Hotel discovery is not configured for airport {airport_code}."
        )

    radius_km = min(max(float(radius_km), 1), 10)
    limit = min(max(int(limit), 1), 100)

    cache_key = (airport_code, round(radius_km, 1))
    cached = OSM_HOTEL_CACHE.get(cache_key)

    if cached and time.time() - cached["timestamp"] < OSM_HOTEL_CACHE_TTL:
        return {
            **cached["result"],
            "data": cached["result"]["data"][:limit]
        }

    lat = airport["latitude"]
    lon = airport["longitude"]
    radius_m = int(radius_km * 1000)

    query = f"""
    [out:json][timeout:20];
    nwr["tourism"~"^(hotel|motel|guest_house)$"]
      (around:{radius_m},{lat},{lon});
    out center;
    """

    request = urllib.request.Request(
        "https://overpass-api.de/api/interpreter",
        data=urllib.parse.urlencode({"data": query}).encode(),
        headers={
            "User-Agent": "AirlineIQ/1.0 (hotel-discovery-poc)",
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded"
        }
    )

    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            osm_result = json.loads(response.read())



    except Exception as exc:
        print(f"OSM hotel discovery error: {exc}")
        print("Falling back to PostgreSQL hotel inventory.")

        try:
            fallback_result = get_nearby_hotels(
                airport_code=airport_code,
                radius_km=radius_km,
                limit=limit
            )

            fallback_result["fallback"] = True
            fallback_result["fallback_reason"] = (
                "OpenStreetMap was temporarily unavailable. "
                "Showing available hotels from local inventory."
            )

            fallback_result["disclaimer"] = (
                "These are simulated hotel inventory records. "
                "Room availability and prices are based on the "
                "local POC database."
            )

            return fallback_result

        except Exception as fallback_exc:
            print(
                f"PostgreSQL hotel fallback error: {fallback_exc}"
            )

            raise HTTPException(
                status_code=503,
                detail=(
                    "Hotel discovery is temporarily unavailable. "
                    "Both OpenStreetMap and local hotel inventory "
                    "could not provide results."
                )
            )


    hotels = []
    seen = set()

    for item in osm_result.get("elements", []):
        tags = item.get("tags", {})

        latitude = item.get("lat")
        longitude = item.get("lon")

        if latitude is None or longitude is None:
            center = item.get("center", {})
            latitude = center.get("lat")
            longitude = center.get("lon")

        if latitude is None or longitude is None:
            continue

        name = tags.get("name") or tags.get("name:en")

        if not name:
            continue

        unique_id = (item.get("type"), item.get("id"))

        if unique_id in seen:
            continue

        seen.add(unique_id)

        lat1 = math.radians(lat)
        lat2 = math.radians(float(latitude))
        delta_lat = math.radians(float(latitude) - lat)
        delta_lon = math.radians(float(longitude) - lon)

        haversine = (
            math.sin(delta_lat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2)
            * math.sin(delta_lon / 2) ** 2
        )

        distance = 6371 * 2 * math.atan2(
            math.sqrt(haversine),
            math.sqrt(1 - haversine)
        )

        if distance > radius_km:
            continue

        address_parts = [
            tags.get("addr:housenumber"),
            tags.get("addr:street"),
            tags.get("addr:suburb"),
            tags.get("addr:city") or tags.get("addr:town"),
            tags.get("addr:postcode")
        ]

        address = ", ".join(
            str(part) for part in address_parts if part
        )

        hotel = {
            "osm_type": item.get("type"),
            "osm_id": item.get("id"),
            "name": name,
            "tourism": tags.get("tourism"),
            "latitude": float(latitude),
            "longitude": float(longitude),
            "distance_km": round(distance, 2),
            "address": address or None,
            "website": tags.get("website") or tags.get("contact:website"),
            "phone": tags.get("phone") or tags.get("contact:phone"),
            "osm_stars": tags.get("stars"),
            "map_url": (
                "https://www.google.com/maps/dir/?api=1"
                f"&origin={lat},{lon}"
                f"&destination={latitude},{longitude}"
                "&travelmode=driving"
            )
        }

        hotels.append(hotel)

    hotels.sort(key=lambda item: item["distance_km"])

    result = {
        "status": "success",
        "inventory_source": "OPENSTREETMAP",
        "airport_code": airport_code,
        "airport_name": airport["name"],
        "city": airport["city"],
        "airport_coordinates": {
            "latitude": lat,
            "longitude": lon
        },
        "radius_km": radius_km,
        "count": len(hotels),
        "data": hotels,
        "disclaimer": (
            "These are OpenStreetMap mapped listings. "
            "Room availability, prices, ratings and reservations "
            "are not verified or provided by OpenStreetMap."
        ),
        "attribution": "© OpenStreetMap contributors",
        "attribution_url": "https://www.openstreetmap.org/copyright"
    }

    OSM_HOTEL_CACHE[cache_key] = {
        "timestamp": time.time(),
        "result": result
    }

    return {
        **result,
        "data": hotels[:limit]
    }


@app.get("/hotels/discover")
def discover_hotels_endpoint(
    airport_code: str = None,
    booking_id: str = None,
    passenger_id: str = None,
    radius_km: float = 15,
    limit: int = 30
):
    if booking_id and passenger_id:
        airport_code = get_passenger_airport(
            booking_id, passenger_id
        )

    if not airport_code:
        raise HTTPException(
            status_code=400,
            detail="Unable to determine the passenger's departure airport."
        )

    return discover_osm_hotels(
        airport_code=airport_code,
        radius_km=radius_km,
        limit=limit
    )


@app.get("/hotels/nearby")
def get_nearby_hotels(
    airport_code: str = None,
    booking_id: str = None,
    passenger_id: str = None,
    radius_km: float = 30,
    limit: int = 30
):
    """
    Discover simulated hotels around the passenger's departure airport.
    """
    if booking_id and passenger_id:
        airport_code = get_passenger_airport(
            booking_id, passenger_id
        )

    if not airport_code:
        raise HTTPException(
            status_code=400,
            detail="Unable to determine the passenger's departure airport."
        )

    airport_code = airport_code.upper()

    airport = HOTEL_AIRPORTS.get(airport_code)

    if not airport:
        raise HTTPException(
            status_code=400,
            detail=f"Hotel discovery is not configured for airport {airport_code}."
        )

    conn = booking_db()

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT *
            FROM hotel_inventory
            WHERE airport_code = %s
              AND available_rooms > 0
            ORDER BY price_per_night ASC
            LIMIT %s
        """, (airport_code, min(max(limit, 1), 100)))

        hotels = []

        for row in cur.fetchall():
            hotel = dict(row)

            distance = hotel_distance_km(
                airport["latitude"],
                airport["longitude"],
                hotel["latitude"],
                hotel["longitude"]
            )

            if distance > radius_km:
                continue

            hotel["distance_km"] = distance
            hotel["airport_name"] = airport["name"]
            hotel["airport_latitude"] = airport["latitude"]
            hotel["airport_longitude"] = airport["longitude"]
            hotel["room_types"] = hotel["room_types"] or []
            hotel["amenities"] = hotel["amenities"] or []
            hotel["price_per_night"] = float(
                hotel["price_per_night"]
            )
            hotel["rating"] = float(hotel["rating"] or 0)
            hotel["available_rooms"] = int(
                hotel["available_rooms"]
            )
            hotel["map_url"] = (
                "https://www.google.com/maps/dir/?api=1"
                f"&origin={airport['latitude']},{airport['longitude']}"
                f"&destination={hotel['latitude']},{hotel['longitude']}"
                "&travelmode=driving"
            )

            hotels.append(hotel)

        hotels.sort(key=lambda item: item["distance_km"])

        return {
            "status": "success",
            "inventory_source": "SIMULATED_POC",
            "airport_code": airport_code,
            "airport_name": airport["name"],
            "city": airport["city"],
            "airport_coordinates": {
                "latitude": airport["latitude"],
                "longitude": airport["longitude"]
            },
            "count": len(hotels),
            "data": hotels
        }

    finally:
        conn.close()



def replace_hotel_booking(data: HotelBookingRequest, old_booking_ref: str):
    """
    Atomically cancel one existing hotel reservation and create its replacement.
    Restores old inventory and deducts new inventory in the same transaction.
    """
    if data.check_out <= data.check_in:
        raise HTTPException(
            status_code=400,
            detail="Check-out must be after check-in."
        )

    nights = (data.check_out - data.check_in).days
    conn = booking_db()

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        lock_key = f"{data.booking_id}:{data.passenger_id}"
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtext(%s))",
            (lock_key,)
        )

        cur.execute("""
            SELECT *
            FROM hotel_bookings
            WHERE booking_ref = %s
              AND booking_id = %s
              AND passenger_id = %s
              AND booking_status = 'CONFIRMED'
              AND check_in < %s
              AND check_out > %s
            FOR UPDATE
        """, (
            old_booking_ref,
            data.booking_id,
            data.passenger_id,
            data.check_out,
            data.check_in
        ))

        old_booking = cur.fetchone()

        if not old_booking:
            raise HTTPException(
                status_code=409,
                detail=(
                    "The selected existing reservation was not found, "
                    "is no longer confirmed, or does not overlap the new dates."
                )
            )

        hotel_ids = sorted({
            old_booking["hotel_id"],
            data.hotel_id
        })

        cur.execute("""
            SELECT hotel_id, hotel_name, available_rooms,
                   price_per_night, currency, room_types,
                   inventory_source
            FROM hotel_inventory
            WHERE hotel_id = ANY(%s)
            ORDER BY hotel_id
            FOR UPDATE
        """, (hotel_ids,))

        inventory = {
            row["hotel_id"]: dict(row)
            for row in cur.fetchall()
        }

        old_hotel = inventory.get(old_booking["hotel_id"])
        new_hotel = inventory.get(data.hotel_id)

        if not old_hotel or not new_hotel:
            raise HTTPException(
                status_code=404,
                detail="Hotel inventory was not found."
            )

        old_room_types = old_hotel["room_types"] or []

        old_room = next(
            (
                item for item in old_room_types
                if isinstance(item, dict)
                and item.get("name") == old_booking["room_type"]
            ),
            None
        )

        if not old_room:
            raise HTTPException(
                status_code=409,
                detail="Unable to restore the old reservation's room inventory."
            )

        old_rooms_count = int(old_booking["rooms"])

        old_hotel["available_rooms"] = (
            int(old_hotel["available_rooms"]) + old_rooms_count
        )

        if "available_rooms" in old_room:
            old_room["available_rooms"] = (
                int(old_room.get("available_rooms", 0))
                + old_rooms_count
            )

        new_room_types = new_hotel["room_types"] or []

        new_room = next(
            (
                item for item in new_room_types
                if isinstance(item, dict)
                and item.get("name") == data.room_type
            ),
            None
        )

        if not new_room:
            raise HTTPException(
                status_code=400,
                detail="The selected room type is not available at this hotel."
            )

        if "available_rooms" in new_room:
            room_remaining = int(new_room.get("available_rooms", 0))

            if room_remaining < data.rooms:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Only {room_remaining} rooms of type "
                        f"{data.room_type} are available."
                    )
                )

            new_room["available_rooms"] = room_remaining - data.rooms

        remaining_rooms = (
            int(new_hotel["available_rooms"]) - data.rooms
        )

        if remaining_rooms < 0:
            raise HTTPException(
                status_code=409,
                detail="Insufficient hotel room inventory."
            )

        new_hotel["available_rooms"] = remaining_rooms

        room_type_remaining = (
            int(new_room["available_rooms"])
            if "available_rooms" in new_room
            else remaining_rooms
        )

        room_price = float(
            new_room.get("price", new_hotel["price_per_night"])
        )

        if room_price <= 0:
            raise HTTPException(
                status_code=400,
                detail="Invalid room price in hotel inventory."
            )

        total = room_price * nights * data.rooms

        for hotel_id, hotel in inventory.items():
            cur.execute("""
                UPDATE hotel_inventory
                SET available_rooms = %s,
                    room_types = %s::jsonb,
                    updated_at = NOW()
                WHERE hotel_id = %s
            """, (
                hotel["available_rooms"],
                json.dumps(hotel["room_types"] or []),
                hotel_id
            ))

        cur.execute("""
            UPDATE hotel_bookings
            SET booking_status = 'CANCELLED'
            WHERE booking_ref = %s
              AND booking_status = 'CONFIRMED'
        """, (old_booking_ref,))

        if cur.rowcount != 1:
            raise HTTPException(
                status_code=409,
                detail="The old reservation could not be cancelled."
            )

        booking_ref = "HTL-" + uuid.uuid4().hex[:10].upper()

        cur.execute("""
            INSERT INTO hotel_bookings (
                booking_ref, booking_id, passenger_id,
                hotel_id, hotel_name, room_type,
                check_in, check_out, nights, guests, rooms,
                price_per_night, total_amount, currency,
                booking_status
            )
            VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                'CONFIRMED'
            )
            RETURNING *
        """, (
            booking_ref,
            data.booking_id,
            data.passenger_id,
            data.hotel_id,
            new_hotel["hotel_name"],
            data.room_type,
            data.check_in,
            data.check_out,
            nights,
            data.guests,
            data.rooms,
            room_price,
            total,
            new_hotel["currency"]
        ))

        booking = dict(cur.fetchone())
        conn.commit()

        return {
            "status": "CONFIRMED",
            "message": "Your replacement hotel reservation is confirmed.",
            "booking": {
                "booking_ref": booking["booking_ref"],
                "booking_id": booking["booking_id"],
                "passenger_id": booking["passenger_id"],
                "hotel_id": booking["hotel_id"],
                "hotel_name": booking["hotel_name"],
                "room_type": booking["room_type"],
                "check_in": str(booking["check_in"]),
                "check_out": str(booking["check_out"]),
                "nights": booking["nights"],
                "guests": booking["guests"],
                "rooms": booking["rooms"],
                "price_per_night": float(booking["price_per_night"]),
                "total_amount": float(booking["total_amount"]),
                "currency": booking["currency"],
                "booking_status": booking["booking_status"]
            },
            "remaining_rooms": remaining_rooms,
            "room_type_remaining_rooms": room_type_remaining
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()



@app.post("/hotels/book")
def book_hotel(data: HotelBookingRequest):
    """
    Atomically reserve simulated hotel rooms and create a booking.
    Supports room-specific prices and availability.
    """
    if data.check_out <= data.check_in:
        raise HTTPException(
            status_code=400,
            detail="Check-out must be after check-in."
        )

    nights = (data.check_out - data.check_in).days
    conn = booking_db()

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Lock the hotel inventory row to prevent concurrent overbooking.
        cur.execute("""
            SELECT hotel_id, hotel_name, available_rooms,
                   price_per_night, currency, room_types,
                   inventory_source
            FROM hotel_inventory
            WHERE hotel_id = %s
            FOR UPDATE
        """, (data.hotel_id,))

        hotel = cur.fetchone()

        if not hotel:
            raise HTTPException(
                status_code=404,
                detail="The selected hotel was not found."
            )

        room_types = hotel["room_types"] or []

        selected_room = next(
            (
                item for item in room_types
                if isinstance(item, dict)
                and item.get("name") == data.room_type
            ),
            None
        )

        if not selected_room:
            raise HTTPException(
                status_code=400,
                detail="The selected room type is not available at this hotel."
            )

        # Use room-specific availability when the inventory provides it.
        # Legacy records continue using the hotel's global room count.
        room_has_separate_inventory = (
            "available_rooms" in selected_room
        )

        if room_has_separate_inventory:
            room_remaining = int(
                selected_room.get("available_rooms", 0)
            )

            if room_remaining < data.rooms:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Only {room_remaining} rooms of type "
                        f"{data.room_type} are available."
                    )
                )

            selected_room["available_rooms"] = (
                room_remaining - data.rooms
            )

            remaining_rooms = int(hotel["available_rooms"]) - data.rooms

            if remaining_rooms < 0:
                raise HTTPException(
                    status_code=409,
                    detail="Insufficient hotel room inventory."
                )

            room_type_remaining = selected_room["available_rooms"]

        else:
            remaining_rooms = int(hotel["available_rooms"])

            if remaining_rooms < data.rooms:
                raise HTTPException(
                    status_code=409,
                    detail="The selected hotel does not have enough rooms."
                )

            remaining_rooms -= data.rooms
            room_type_remaining = remaining_rooms

        room_price = float(
            selected_room.get(
                "price",
                hotel["price_per_night"]
            )
        )

        if room_price <= 0:
            raise HTTPException(
                status_code=400,
                detail="Invalid room price in hotel inventory."
            )

        total = room_price * nights * data.rooms

        # Update inventory while the row lock is still held.
        cur.execute("""
            UPDATE hotel_inventory
            SET available_rooms = %s,
                room_types = %s::jsonb,
                updated_at = NOW()
            WHERE hotel_id = %s
        """, (
            remaining_rooms,
            json.dumps(room_types),
            data.hotel_id
        ))

        booking_ref = "HTL-" + uuid.uuid4().hex[:10].upper()

        cur.execute("""
            INSERT INTO hotel_bookings (
                booking_ref, booking_id, passenger_id,
                hotel_id, hotel_name, room_type,
                check_in, check_out, nights, guests, rooms,
                price_per_night, total_amount, currency,
                booking_status
            )
            VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                'CONFIRMED'
            )
            RETURNING *
        """, (
            booking_ref,
            data.booking_id,
            data.passenger_id,
            data.hotel_id,
            hotel["hotel_name"],
            data.room_type,
            data.check_in,
            data.check_out,
            nights,
            data.guests,
            data.rooms,
            room_price,
            total,
            hotel["currency"]
        ))

        booking = dict(cur.fetchone())
        conn.commit()

        return {
            "status": "CONFIRMED",
            "message": "Your simulated hotel reservation is confirmed.",
            "booking": {
                "booking_ref": booking["booking_ref"],
                "booking_id": booking["booking_id"],
                "passenger_id": booking["passenger_id"],
                "hotel_id": booking["hotel_id"],
                "hotel_name": booking["hotel_name"],
                "room_type": booking["room_type"],
                "check_in": str(booking["check_in"]),
                "check_out": str(booking["check_out"]),
                "nights": booking["nights"],
                "guests": booking["guests"],
                "rooms": booking["rooms"],
                "price_per_night": float(
                    booking["price_per_night"]
                ),
                "total_amount": float(
                    booking["total_amount"]
                ),
                "currency": booking["currency"],
                "booking_status": booking["booking_status"]
            },
            "remaining_rooms": remaining_rooms,
            "room_type_remaining_rooms": room_type_remaining
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        conn.close()


@app.get("/hotels/bookings/{booking_ref}")
def get_hotel_booking(booking_ref: str):
    conn = booking_db()

    try:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("""
            SELECT *
            FROM hotel_bookings
            WHERE booking_ref = %s
        """, (booking_ref.upper(),))

        row = cur.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail="Hotel booking not found."
            )

        result = dict(row)

        for field in ("check_in", "check_out", "created_at"):
            if result.get(field):
                result[field] = str(result[field])

        for field in ("price_per_night", "total_amount"):
            result[field] = float(result[field])

        return {"status": "success", "booking": result}

    finally:
        conn.close()


# Create hotel tables when the API module starts.
initialize_hotel_tables()

# ============================================================
# END AIRLINEIQ HOTEL MODULE
# ============================================================
