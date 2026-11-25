import os
import psycopg2
from psycopg2.extras import Json
from dotenv import load_dotenv

load_dotenv("/home/airline-ai/.env")

conn = psycopg2.connect(
    host="127.0.0.1",
    port=5432,
    dbname="airline",
    user="postgres",
    password=os.getenv("AIRLINE_PG_PASSWORD", "change-me")
)

hotels = [
    # BENGALURU - BLR
    {
        "id": "BLR-HTL-001",
        "name": "AeroNest Airport Suites",
        "city": "Bengaluru",
        "airport": "BLR",
        "lat": 13.1980, "lon": 77.6950,
        "rooms": 18, "price": 4200, "rating": 4.4,
        "image": "https://images.unsplash.com/photo-1566073771259-6a8506099945?w=900",
        "types": [
            {"name": "Standard Room", "price": 4200},
            {"name": "Deluxe Room", "price": 5600},
            {"name": "Executive Suite", "price": 7800}
        ],
        "amenities": ["Free Wi-Fi", "Airport Shuttle", "Restaurant", "24-hour Reception", "Parking"]
    },
    {
        "id": "BLR-HTL-002",
        "name": "Runway View Business Hotel",
        "city": "Bengaluru",
        "airport": "BLR",
        "lat": 13.2100, "lon": 77.7000,
        "rooms": 12, "price": 5100, "rating": 4.6,
        "image": "https://images.unsplash.com/photo-1542314831-068cd1dbfeeb?w=900",
        "types": [
            {"name": "Business Room", "price": 5100},
            {"name": "Premium Room", "price": 6500}
        ],
        "amenities": ["Free Wi-Fi", "Airport Shuttle", "Gym", "Restaurant", "Conference Room"]
    },
    {
        "id": "BLR-HTL-003",
        "name": "Terminal Comfort Residency",
        "city": "Bengaluru",
        "airport": "BLR",
        "lat": 13.1840, "lon": 77.7130,
        "rooms": 9, "price": 3200, "rating": 4.1,
        "image": "https://images.unsplash.com/photo-1551882547-ff40c63fe5fa?w=900",
        "types": [
            {"name": "Standard Room", "price": 3200},
            {"name": "Family Room", "price": 4700}
        ],
        "amenities": ["Free Wi-Fi", "Restaurant", "Parking", "Room Service"]
    },
    {
        "id": "BLR-HTL-004",
        "name": "Skyline Airport Retreat",
        "city": "Bengaluru",
        "airport": "BLR",
        "lat": 13.2250, "lon": 77.6900,
        "rooms": 15, "price": 6900, "rating": 4.8,
        "image": "https://images.unsplash.com/photo-1571896349842-33c89424de2d?w=900",
        "types": [
            {"name": "Deluxe Room", "price": 6900},
            {"name": "Luxury Suite", "price": 9500}
        ],
        "amenities": ["Free Wi-Fi", "Pool", "Spa", "Airport Shuttle", "Restaurant", "Gym"]
    },

    # MUMBAI - BOM
    {
        "id": "BOM-HTL-001",
        "name": "AeroHarbor City Hotel",
        "city": "Mumbai",
        "airport": "BOM",
        "lat": 19.0960, "lon": 72.8650,
        "rooms": 16, "price": 4800, "rating": 4.3,
        "image": "https://images.unsplash.com/photo-1566073771259-6a8506099945?w=900",
        "types": [
            {"name": "Standard Room", "price": 4800},
            {"name": "Deluxe Room", "price": 6200}
        ],
        "amenities": ["Free Wi-Fi", "Airport Shuttle", "Restaurant", "Parking"]
    },
    {
        "id": "BOM-HTL-002",
        "name": "Terminal One Executive Stay",
        "city": "Mumbai",
        "airport": "BOM",
        "lat": 19.1030, "lon": 72.8730,
        "rooms": 10, "price": 6200, "rating": 4.7,
        "image": "https://images.unsplash.com/photo-1542314831-068cd1dbfeeb?w=900",
        "types": [
            {"name": "Executive Room", "price": 6200},
            {"name": "Premium Suite", "price": 8900}
        ],
        "amenities": ["Free Wi-Fi", "Airport Shuttle", "Gym", "Restaurant", "Room Service"]
    },
    {
        "id": "BOM-HTL-003",
        "name": "Mumbai Transit Comfort",
        "city": "Mumbai",
        "airport": "BOM",
        "lat": 19.0810, "lon": 72.8580,
        "rooms": 14, "price": 3500, "rating": 4.0,
        "image": "https://images.unsplash.com/photo-1551882547-ff40c63fe5fa?w=900",
        "types": [
            {"name": "Standard Room", "price": 3500},
            {"name": "Twin Room", "price": 4300}
        ],
        "amenities": ["Free Wi-Fi", "Restaurant", "24-hour Reception", "Parking"]
    },
    {
        "id": "BOM-HTL-004",
        "name": "Western Gateway Suites",
        "city": "Mumbai",
        "airport": "BOM",
        "lat": 19.1150, "lon": 72.8670,
        "rooms": 8, "price": 7900, "rating": 4.8,
        "image": "https://images.unsplash.com/photo-1571896349842-33c89424de2d?w=900",
        "types": [
            {"name": "Deluxe Room", "price": 7900},
            {"name": "Luxury Suite", "price": 11200}
        ],
        "amenities": ["Free Wi-Fi", "Pool", "Spa", "Restaurant", "Airport Shuttle"]
    },

    # DELHI - DEL
    {
        "id": "DEL-HTL-001",
        "name": "Capital Airport Suites",
        "city": "Delhi",
        "airport": "DEL",
        "lat": 28.5480, "lon": 77.0950,
        "rooms": 11, "price": 4800, "rating": 4.4,
        "image": "https://images.unsplash.com/photo-1566073771259-6a8506099945?w=900",
        "types": [
            {"name": "Standard Room", "price": 4800},
            {"name": "Deluxe Room", "price": 6200}
        ],
        "amenities": ["Free Wi-Fi", "Airport Shuttle", "Restaurant", "Parking"]
    },
    {
        "id": "DEL-HTL-002",
        "name": "Runway Executive Residency",
        "city": "Delhi",
        "airport": "DEL",
        "lat": 28.5630, "lon": 77.0900,
        "rooms": 13, "price": 5900, "rating": 4.6,
        "image": "https://images.unsplash.com/photo-1542314831-068cd1dbfeeb?w=900",
        "types": [
            {"name": "Business Room", "price": 5900},
            {"name": "Executive Suite", "price": 8500}
        ],
        "amenities": ["Free Wi-Fi", "Gym", "Restaurant", "Airport Shuttle"]
    },
    {
        "id": "DEL-HTL-003",
        "name": "Delhi Transit Lodge",
        "city": "Delhi",
        "airport": "DEL",
        "lat": 28.5410, "lon": 77.1080,
        "rooms": 20, "price": 3100, "rating": 4.0,
        "image": "https://images.unsplash.com/photo-1551882547-ff40c63fe5fa?w=900",
        "types": [
            {"name": "Standard Room", "price": 3100},
            {"name": "Family Room", "price": 4600}
        ],
        "amenities": ["Free Wi-Fi", "Restaurant", "Parking", "Room Service"]
    },
    {
        "id": "DEL-HTL-004",
        "name": "Skyline Capital Retreat",
        "city": "Delhi",
        "airport": "DEL",
        "lat": 28.5720, "lon": 77.1050,
        "rooms": 7, "price": 8200, "rating": 4.8,
        "image": "https://images.unsplash.com/photo-1571896349842-33c89424de2d?w=900",
        "types": [
            {"name": "Deluxe Room", "price": 8200},
            {"name": "Luxury Suite", "price": 11500}
        ],
        "amenities": ["Free Wi-Fi", "Pool", "Spa", "Restaurant", "Airport Shuttle"]
    }
]

try:
    cur = conn.cursor()

    for h in hotels:
        cur.execute("""
            INSERT INTO hotel_inventory (
                hotel_id, hotel_name, city, airport_code,
                latitude, longitude, available_rooms,
                price_per_night, currency, room_types,
                amenities, rating, image_url, inventory_source
            )
            VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,'INR',
                %s,%s,%s,%s,'SIMULATED'
            )
            ON CONFLICT (hotel_id) DO UPDATE SET
                hotel_name = EXCLUDED.hotel_name,
                city = EXCLUDED.city,
                airport_code = EXCLUDED.airport_code,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                price_per_night = EXCLUDED.price_per_night,
                room_types = EXCLUDED.room_types,
                amenities = EXCLUDED.amenities,
                rating = EXCLUDED.rating,
                image_url = EXCLUDED.image_url,
                updated_at = NOW()
        """, (
            h["id"], h["name"], h["city"], h["airport"],
            h["lat"], h["lon"], h["rooms"], h["price"],
            Json(h["types"]), Json(h["amenities"]),
            h["rating"], h["image"]
        ))

    conn.commit()
    print(f"Seeded/updated {len(hotels)} simulated hotel records.")
    print("Existing room counts are preserved on repeated runs.")

finally:
    conn.close()
