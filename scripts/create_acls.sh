#!/bin/bash
set -euo pipefail

SA_ID="$1"

TOPICS=(
  flight_status
  flight_telemetry
  passenger_itineraries
  booking_events
  airport_events
  hotel_inventory
  passenger_state
  rebooking_events
)

for topic in "${TOPICS[@]}"; do
    echo "Configuring ACLs for $topic"

    confluent kafka acl create \
      --allow \
      --service-account "$SA_ID" \
      --operations create,describe,read,write \
      --topic "$topic"
done

echo "All topic ACLs configured."
