import {
  MapContainer,
  TileLayer,
  CircleMarker,
  Popup
} from 'react-leaflet'

import 'leaflet/dist/leaflet.css'

const AIRPORT_COORDINATES = {
  BLR: { latitude: 13.1986, longitude: 77.7066 },
  BOM: { latitude: 19.0896, longitude: 72.8656 },
  DEL: { latitude: 28.5562, longitude: 77.1000 }
}

export default function HotelResults({ hotels = [], discovery = null }) {
  if (!hotels.length || !discovery) return null

  const airport =
    discovery.airport_coordinates ||
    AIRPORT_COORDINATES[discovery.airport_code] ||
    {}

  const hasAirportCoordinates =
    Number.isFinite(Number(airport.latitude)) &&
    Number.isFinite(Number(airport.longitude))

  const validMapHotels = hotels.filter(hotel =>
    Number.isFinite(Number(hotel.latitude)) &&
    Number.isFinite(Number(hotel.longitude))
  )

  return (
    <div className="hotel-results">
      <div className="hotel-results-heading">
        <h3>Hotels near {discovery.airport_name}</h3>
        <span>{hotels.length} mapped listings</span>
      </div>

      <p className="hotel-results-disclaimer">
        These are OpenStreetMap listings. Live room availability,
        prices and reservations are not verified.
      </p>

      {hasAirportCoordinates && (
      <div className="hotel-map-container">
        <MapContainer
          center={[airport.latitude, airport.longitude]}
          zoom={11}
          scrollWheelZoom={false}
          className="hotel-map"
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a>'
            url="https://tile.openstreetmap.org/{z}/{x}/{y}.png"
          />

          <CircleMarker
            center={[airport.latitude, airport.longitude]}
            radius={9}
            pathOptions={{
              color: '#174ea6',
              fillColor: '#174ea6',
              fillOpacity: 1
            }}
          >
            <Popup>
              <b>{discovery.airport_name}</b>
              <br />
              Airport
            </Popup>
          </CircleMarker>

          {validMapHotels.map(hotel => (
            <CircleMarker
              key={`${hotel.osm_type}-${hotel.osm_id}`}
              center={[hotel.latitude, hotel.longitude]}
              radius={7}
              pathOptions={{
                color: '#15803d',
                fillColor: '#22c55e',
                fillOpacity: 0.85
              }}
            >
              <Popup>
                <b>{hotel.name}</b>
                <br />
                {hotel.distance_km} km from airport
                <br />
                {hotel.tourism?.replaceAll('_', ' ')}
                <br />
                <a
                  href={hotel.map_url}
                  target="_blank"
                  rel="noreferrer"
                >
                  Directions
                </a>
              </Popup>
            </CircleMarker>
          ))}
        </MapContainer>
      </div>
      )}

      <div className="hotel-results-list">
        {hotels.map(hotel => (
          <div
            className="hotel-result-card"
            key={`${hotel.osm_type}-${hotel.osm_id}`}
          >
            <div className="hotel-result-info">
              <h4>{hotel.name}</h4>

              <span className="hotel-result-type">
                {hotel.tourism?.replaceAll('_', ' ')}
              </span>

              <p>
                <strong>{hotel.distance_km} km</strong> from airport
              </p>

              {hotel.address && <p>{hotel.address}</p>}

              {hotel.phone && <p>Phone: {hotel.phone}</p>}

              {hotel.osm_stars && (
                <p>OSM-tagged stars: {hotel.osm_stars}</p>
              )}
            </div>

            <div className="hotel-result-actions">
              {hotel.website && (
                <a
                  href={hotel.website}
                  target="_blank"
                  rel="noreferrer"
                >
                  Website
                </a>
              )}

              <a
                href={hotel.map_url}
                target="_blank"
                rel="noreferrer"
              >
                Directions
              </a>
            </div>
          </div>
        ))}
      </div>

      <div className="hotel-osm-attribution">
        Data: <a
          href={discovery.attribution_url}
          target="_blank"
          rel="noreferrer"
        >
          {discovery.attribution || '© OpenStreetMap contributors'}
        </a>
      </div>
    </div>
  )
}
