# Airline AI – Intelligent Passenger Assistance & Rebooking Platform

An AI-powered airline passenger assistance platform that combines Large Language Models (LLMs), real-time event streaming, stream processing, and conversational workflows to improve the passenger experience during flight disruptions.

The platform helps passengers access flight information, explore alternative flights, manage rebooking workflows, select seats, and discover nearby hotels through an AI-powered conversational interface.

---

## 1. Project Overview

Airline operations involve continuously changing flight schedules, delays, cancellations, passenger itineraries, and seat availability. Passengers often need to interact with multiple systems to understand disruptions and identify suitable alternatives.

This project demonstrates an integrated Airline AI solution that brings together:

- AI-powered passenger conversations.
- Real-time flight and passenger event processing.
- Intelligent flight-status and disruption assistance.
- Alternative flight discovery and rebooking.
- Seat availability and booking management.
- Nearby hotel discovery.
- Conversational hotel booking workflows.
- Event-driven data integration using Confluent Cloud.

The solution combines AI-driven natural-language interaction with deterministic backend business logic to deliver a unified passenger assistance experience.

## 2. Key Features

### AI-Powered Passenger Assistant

- Conversational interface for passenger queries.
- Natural-language understanding using IBM Granite.
- Intent identification and context-aware response generation.
- Assistance with flight status, disruptions, alternative flights, and hotel-related requests.
- Backend orchestration of AI and operational data.

### Real-Time Flight Information

- Simulated flight-status event generation.
- Flight information and operational updates.
- Passenger itinerary event generation.
- Streaming integration through Confluent Cloud.
- Access to flight-related operational context.

### Alternative Flight Discovery

- Alternative flight simulation.
- Alternative flight availability management.
- Retrieval of suitable flight options.
- Passenger selection of an alternative flight.
- Seat availability validation.

### Flight Rebooking and Seat Management

- Passenger booking lookup.
- Alternative flight selection.
- Seat-map selection.
- Seat availability verification.
- Transactional seat reservation.
- Booking confirmation and PNR generation.
- Cancellation and seat-release handling.
- Protection against conflicting seat reservations through database transactions and locking.

### Hotel Discovery

- Nearby hotel discovery using OpenStreetMap and Overpass API.
- Airport-based hotel discovery.
- Hotel information including name, location, distance, and available mapped details.
- Fallback to locally maintained hotel inventory when external discovery is unavailable.

### Conversational Hotel Booking

- Hotel selection through the AI assistant.
- Room-type selection.
- Check-in and check-out date collection.
- Guest and room selection.
- Hotel booking confirmation.
- Existing overlapping reservation detection.
- Replacement workflow for conflicting hotel reservations.

Hotel booking is currently implemented against simulated inventory. OpenStreetMap discovery provides mapped hotel information and does not independently confirm live room availability or real-world reservations.

## 3. Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Frontend | React | Passenger-facing user interface |
| Frontend tooling | Vite | Frontend development and build |
| API communication | Axios | Frontend-to-backend communication |
| Backend | Python 3.11, FastAPI | API development and business logic |
| API server | Uvicorn | ASGI application server |
| AI / LLM | IBM Granite | Natural-language understanding and response generation |
| AI platform | IBM watsonx.ai | Granite model integration |
| AI integration | Model Context Protocol (MCP) | Standardized access to exposed tools and data |
| Streaming platform | Confluent Cloud | Managed event streaming and Kafka-compatible topics |
| Stream processing | Apache Flink | Processing streaming events and passenger-state data |
| Database | PostgreSQL | Operational data and transactional booking persistence |
| Hotel discovery | OpenStreetMap | Geographic hotel information |
| External data query | Overpass API | Retrieval of mapped hotel information |
| Maps | Leaflet / React-Leaflet | Hotel location visualization |
| Charts | Recharts | Frontend data visualization |
| Validation | Pydantic | Backend request and data validation |
| Deployment | Linux, Docker, systemd | Application and service deployment |

**Streaming platform clarification:** The project uses Confluent Cloud as its managed Kafka-compatible streaming platform. It does not depend on a self-managed Apache Kafka broker cluster for this implementation.

## 4. High-Level Architecture

```text
                  PASSENGER
                      |
                      v
              REACT FRONTEND
                      |
                Axios / REST
                      |
                      v
               FASTAPI BACKEND
                      |
          +-----------+------------+
          |           |            |
          v           v            v
      GRANITE       MCP        BUSINESS LOGIC
       LLM        INTEGRATION   AND WORKFLOWS
          |           |
          |           v
          |     CONFLUENT CLOUD
          |           |
          |     KAFKA TOPICS
          |           |
          |           v
          |      APACHE FLINK
          |           |
          |           v
          |    PROCESSED EVENTS
          |     / DERIVED STATE
          |           |
          +-----------+
                      |
                      v
                POSTGRESQL
                      |
                      v
              PASSENGER RESPONSE
```

The architecture is event-driven, with FastAPI coordinating passenger interactions, AI integration, data access, and transactional business operations.

Not every operation passes through the streaming platform. Operations such as seat selection, booking confirmation, and hotel inventory updates are handled by backend services and PostgreSQL transactions.

## 5. Event Streaming Architecture

Confluent Cloud is used as the managed streaming platform for distributing flight and passenger-related events.

Python-based simulators generate operational events and publish them to the configured streaming topics.

### Main Streaming Topics

| Topic | Purpose |
|---|---|
| `flight_status` | Flight status and operational updates |
| `alternative_flights` | Alternative flight information |
| `passenger_itineraries` | Passenger itinerary events |
| `passenger_state_stream` | Processed passenger-state information |
| `hotel_inventory` | Hotel inventory-related information |

### Event Processing Flow

1. Python simulators generate flight, passenger, and alternative-flight events.
2. Events are published to Confluent Cloud.
3. Stream-processing jobs consume relevant events.
4. Apache Flink processes passenger and operational information.
5. Processed passenger-state information is made available through a Kafka-compatible topic.
6. Backend integrations retrieve relevant operational context for passenger assistance.

The Confluent Cloud environment also integrates with Schema Registry for event schema management.

## 6. AI and MCP Integration

### IBM Granite LLM

IBM Granite is integrated through IBM watsonx.ai.

The LLM supports:

- Natural-language understanding.
- Passenger request interpretation.
- Intent classification.
- Conversational response generation.
- Context-aware passenger assistance.

The model provides the language intelligence layer of the application.

### Model Context Protocol (MCP)

The project includes a Confluent MCP integration to expose Kafka-related data and operations to the backend.

The integration includes operations such as:

- `listTopics` – discover available topics.
- `getMetadata` – retrieve topic metadata.
- `queryData` – retrieve data exposed through the MCP integration.

The backend uses MCP-related functionality to access relevant operational context, including passenger state, flight status, and hotel inventory.

### How LLM and MCP Work Together

- Granite helps understand what the passenger is asking.
- MCP provides a standardized mechanism to access exposed tools and streaming data.
- FastAPI coordinates these components.
- PostgreSQL and backend business logic handle operational transactions.

MCP is an integration mechanism, not an LLM or a replacement for Confluent Cloud.

## 7. Flight Rebooking Workflow

The flight rebooking workflow combines event-driven flight information with transactional booking operations.

```text
Passenger Request
       |
       v
Flight / Booking Lookup
       |
       v
Retrieve Flight Context
       |
       v
Identify Disruption
       |
       v
Retrieve Alternative Flights
       |
       v
Passenger Selects Flight
       |
       v
Display Available Seats
       |
       v
Passenger Selects Seat
       |
       v
Validate Seat Availability
       |
       v
Confirm Booking
       |
       v
PostgreSQL Transaction
       |
       v
Generate PNR
       |
       v
Return Confirmation
```

The booking workflow validates seat availability before confirmation and uses transactional database operations to maintain booking consistency.

## 8. Hotel Discovery and Booking Workflow

The hotel functionality combines external geographic discovery with locally maintained inventory.

### Hotel Discovery

1. Identify the relevant passenger journey and airport.
2. Query OpenStreetMap data using the Overpass API.
3. Retrieve mapped hotels around the airport.
4. Present relevant hotel information.
5. Use PostgreSQL hotel inventory as a fallback when external discovery is unavailable.

### Hotel Booking

1. Passenger requests hotel assistance.
2. The application presents hotel options.
3. Passenger selects a hotel.
4. The assistant collects room type and booking details.
5. Inventory is validated.
6. Booking is confirmed against the simulated hotel inventory.
7. Existing overlapping reservations can trigger a replacement confirmation workflow.

**Note:** Hotel discovery data is sourced from OpenStreetMap. The current hotel booking workflow uses simulated inventory and should not be interpreted as a live integration with hotel reservation providers.

## 9. Database

PostgreSQL is used as the operational database for transactional application data.

Key database entities include:

| Table | Purpose |
|---|---|
| `flight_inventory` | Alternative flight inventory and seat availability |
| `flight_seats` | Individual flight seat records |
| `flight_state` | Current flight operational state |
| `passenger_bookings` | Passenger flight booking records |
| `hotel_inventory` | Simulated hotel and room inventory |
| `hotel_bookings` | Hotel booking and reservation records |

PostgreSQL provides persistent storage for booking operations and inventory management.

## 10. Project Structure

The repository is organized into the following major components:

```text
airline-ai/
│
├── ai-agent/
│
├── backend/
│
├── frontend/
│
├── mcp-server/
│
├── flink/
│
├── event-simulator/
│
├── infrastructure/
│
├── config/
│
├── database/
│
├── data/
│
├── schemas/
│
├── scripts/
│
├── webhook/
│
├── lakehouse/
│
├── logs/
│
├── airline_ingestion_simulator.py
│
├── alternative_flights_simulator.py
│
└── seed_hotel_inventory.py
```

### Main Components

- `ai-agent/` – AI assistant-related components.
- `backend/` – FastAPI application, APIs, and business workflows.
- `frontend/` – React-based passenger portal.
- `mcp-server/` – MCP integration components.
- `flink/` – Stream-processing jobs and related configurations.
- `event-simulator/` – Event simulation components.
- `infrastructure/` – Infrastructure and deployment-related resources.
- `config/` – Application configuration templates and settings.
- `database/` – Database-related scripts and definitions.
- `data/` – Project data and supporting datasets.
- `schemas/` – Event and data schema definitions.
- `scripts/` – Utility and execution scripts.
- `webhook/` – Webhook-related components.
- `lakehouse/` – Lakehouse-related project components.

## 11. Current Implementation Status

| Component | Status |
|---|---|
| React passenger portal | Implemented |
| FastAPI backend | Implemented |
| IBM Granite integration | Integrated |
| Confluent MCP integration | Integrated |
| Confluent Cloud event streaming | Integrated |
| Python event simulators | Implemented |
| Apache Flink processing | Integrated |
| PostgreSQL operational database | Implemented |
| Flight status workflow | Implemented |
| Alternative flight discovery | Implemented |
| Seat selection and validation | Implemented |
| Flight booking and PNR generation | Demonstrated |
| OpenStreetMap hotel discovery | Implemented |
| Hotel booking workflow | Implemented; further end-to-end validation required |
| Hotel reservation replacement logic | Implemented; further testing required |
| Live third-party hotel reservation integration | Not implemented |

## 12. Configuration and Security

Application credentials and environment-specific configuration must be provided through environment variables or secure configuration mechanisms.

Examples of configuration categories:

- Confluent Cloud bootstrap and authentication settings.
- Schema Registry connection settings.
- IBM watsonx.ai model credentials and endpoint configuration.
- MCP endpoint and authentication settings.
- PostgreSQL connection configuration.
- Application service configuration.

**Security guidelines:**

- Never commit `.env` files or credentials.
- Do not include API keys, passwords, private certificates, or authentication tokens in the repository.
- Use `.env.example` with placeholder values for documenting required configuration.
- Keep production configuration outside source control.
- Review files before every Git commit.

## 13. Future Enhancements

Potential improvements include:

- Integration with live airline and reservation-provider APIs.
- Production-grade hotel booking integration.
- Persistent conversational session management.
- Enhanced observability, logging, and monitoring.
- Improved retry, caching, and resilience mechanisms for external hotel discovery.
- Advanced passenger-risk and disruption prediction.
- Expanded lakehouse and analytical data integration.
- Production-grade authentication, authorization, and deployment architecture.
- Additional testing and automated CI/CD pipelines.

## 14. Disclaimer

This project is a Proof of Concept (POC) demonstrating AI-powered airline passenger assistance and event-driven operational workflows.

Flight events, alternative flights, and booking scenarios may use simulated data. Flight and hotel booking confirmations within the POC are application-level demonstrations and do not represent actual reservations with commercial airlines or hotel providers.

External hotel discovery is based on mapped OpenStreetMap information and does not guarantee availability or pricing.


