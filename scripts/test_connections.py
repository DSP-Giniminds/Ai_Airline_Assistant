import os
import sys
import requests
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient

ENV_FILE = "/home/airline-ai/.env"
load_dotenv(ENV_FILE)

required = [
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_API_KEY",
    "KAFKA_API_SECRET",
    "SCHEMA_REGISTRY_URL",
    "SCHEMA_REGISTRY_API_KEY",
    "SCHEMA_REGISTRY_API_SECRET",
]

missing = [key for key in required if not os.getenv(key)]

if missing:
    print("MISSING CONFIGURATION:", ", ".join(missing))
    sys.exit(1)

print("\n========== KAFKA CONNECTION TEST ==========")

config = {
    "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": os.environ["KAFKA_API_KEY"],
    "sasl.password": os.environ["KAFKA_API_SECRET"],
    "client.id": "airline-ai-connectivity-test",
}

try:
    admin = AdminClient(config)
    metadata = admin.list_topics(timeout=20)

    print("KAFKA CONNECTION: SUCCESS")
    print("Broker count:", len(metadata.brokers))
    print("Existing topic count:", len(metadata.topics))

except Exception as exc:
    print("KAFKA CONNECTION: FAILED")
    print(type(exc).__name__, str(exc))
    sys.exit(2)

print("\n========== SCHEMA REGISTRY TEST ==========")

url = os.environ["SCHEMA_REGISTRY_URL"].rstrip("/") + "/subjects"

try:
    response = requests.get(
        url,
        auth=(
            os.environ["SCHEMA_REGISTRY_API_KEY"],
            os.environ["SCHEMA_REGISTRY_API_SECRET"],
        ),
        timeout=20,
    )

    response.raise_for_status()

    print("SCHEMA REGISTRY: SUCCESS")
    print("Existing subject count:", len(response.json()))

except Exception as exc:
    print("SCHEMA REGISTRY: FAILED")
    print(type(exc).__name__, str(exc))
    sys.exit(3)

print("\nALL CONNECTION TESTS PASSED")
