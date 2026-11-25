from pathlib import Path
import shutil

path = Path("/home/airline-ai/backend/rebooking_api.py")
source = path.read_text()

backup = path.with_suffix(".py.before_alternatives")
shutil.copy2(path, backup)

# 1. Add alternative flight API after get_flights()
marker = '''# ============================================================
# PASSENGER CONNECTION RISK
# ============================================================
'''

endpoint = '''
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


'''

if endpoint not in source:
    if marker not in source:
        raise SystemExit("Could not locate passenger section. No patch applied.")
    source = source.replace(marker, endpoint + marker, 1)

# 2. Extend Granite intent classification
source = source.replace(
'''hotel - hotel availability, accommodation or hotel prices
recovery - rebooking, missed flights or requests for travel assistance''',
'''hotel - hotel availability, accommodation or hotel prices
alternative_flights - requests to find, show, compare or check alternative flights
recovery - rebooking, missed flights or requests for travel assistance'''
)

source = source.replace(
'''allowed = {"general", "flight", "connection", "hotel", "recovery"}''',
'''allowed = {
        "general", "flight", "connection", "hotel",
        "alternative_flights", "recovery"
    }'''
)

# 3. Explicitly identify alternative-flight discovery requests
old = '''model = get_granite_model()
    intent = classify_passenger_intent(model, message)'''

new = '''model = get_granite_model()

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
        intent = classify_passenger_intent(model, message)'''

if old not in source:
    raise SystemExit("Could not locate intent classification. No patch applied.")

source = source.replace(old, new, 1)

# 4. Retrieve alternatives using the passenger's original route
marker = '''    if intent in {"hotel", "recovery"}:
'''

alternative_context = '''    if intent == "alternative_flights":
        original_flight = context.get("flight") or {}

        origin = original_flight.get("ORIGIN", "BOM")
        destination = original_flight.get("DESTINATION", "DEL")

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
                    "flights with available seats at the moment. "
                    "Please try again shortly."
                ),
                "context": context
            }

        reply = generate_agent_response(
            model, message, intent, context
        )

        return {
            "status": "success",
            "intent": intent,
            "reply": reply,
            "context": context
        }

'''

if alternative_context not in source:
    if marker not in source:
        raise SystemExit("Could not locate hotel context section. No patch applied.")
    source = source.replace(marker, alternative_context + marker, 1)

# 5. Do not treat flight discovery as an actual rebooking request
source = source.replace(
'''        "change flight", "book another flight", "alternative flight"
''',
'''        "change flight", "book another flight"
'''
)

path.write_text(source)
print("Alternative flight backend patch applied successfully.")
print(f"Backup: {backup}")
