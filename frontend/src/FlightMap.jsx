import { useState, useEffect } from 'react'
import {
  MapContainer, TileLayer, Marker, Polyline, Popup, useMap
} from 'react-leaflet'
import L from 'leaflet'
import { Plane, Navigation, ChevronDown } from 'lucide-react'
import 'leaflet/dist/leaflet.css'
import './FlightMap.css'

const airports = {
  BOM: { name: 'Mumbai', coords: [19.0896, 72.8656] },
  DEL: { name: 'Delhi', coords: [28.5562, 77.1000] },
  BLR: { name: 'Bengaluru', coords: [13.1986, 77.7066] },
  HYD: { name: 'Hyderabad', coords: [17.2403, 78.4294] },
  MAA: { name: 'Chennai', coords: [12.9941, 80.1709] },
  CCU: { name: 'Kolkata', coords: [22.6547, 88.4467] }
}

const airportIcon = (code, color) => L.divIcon({
  className: 'airport-marker-wrap',
  html: `<div class="airport-marker" style="--marker-color:${color}"><span></span><b>${code}</b></div>`,
  iconSize: [54, 48],
  iconAnchor: [27, 24]
})

const aircraftIcon = L.divIcon({
  className: 'aircraft-marker-wrap',
  html: '<div class="aircraft-emoji">✈️</div>',
  iconSize: [48, 48],
  iconAnchor: [24, 24]
})

function FitRoute({from, to}) {
  const map = useMap()
  useEffect(() => {
    map.fitBounds([from, to], { padding: [65, 65] })
  }, [map, from, to])
  return null
}

export default function FlightMap({flights = []}) {
  const [selected, setSelected] = useState('')
  const [showDetails, setShowDetails] = useState(true)

  useEffect(() => {
    if (flights.length && !flights.some(f => f.flight_id === selected)) {
      setSelected(flights[0].flight_id)
    }
  }, [flights, selected])

  const flight = flights.find(f => f.flight_id === selected)
  const from = flight && airports[flight.origin]
  const to = flight && airports[flight.destination]

  if (!flights.length) {
    return (
      <section className="panel flight-map-panel">
        <div className="panel-heading"><div><h3>Flight tracking map</h3><p>Waiting for live flight records</p></div></div>
        <p className="muted">No flight data is currently available from Kafka.</p>
      </section>
    )
  }

  if (!from || !to) {
    return (
      <section className="panel flight-map-panel">
        <div className="panel-heading"><div><h3>Flight tracking map</h3><p>Route coordinates unavailable for this airport pair</p></div></div>
        <p className="muted">{flight?.origin} → {flight?.destination}</p>
      </section>
    )
  }

  const midpoint = [
    (from.coords[0] + to.coords[0]) / 2,
    (from.coords[1] + to.coords[1]) / 2
  ]

  return (
    <section className="panel flight-map-panel">
      <div className="panel-heading map-heading">
        <div>
          <div className="map-eyebrow"><Navigation size={13}/> ROUTE MONITOR</div>
          <h3>Flight tracking map</h3>
          <p>Routes based on live flight status records</p>
        </div>
        <div className="map-controls">
          <select value={selected} onChange={e => setSelected(e.target.value)}>
            {flights.map(f => (
              <option key={f.flight_id} value={f.flight_id}>
                {f.flight_id} · {f.origin} → {f.destination}
              </option>
            ))}
          </select>
          <button onClick={() => setShowDetails(!showDetails)}>
            {showDetails ? 'Hide details' : 'Flight details'} <ChevronDown size={14}/>
          </button>
        </div>
      </div>

      {showDetails && (
        <div className="map-flight-summary">
          <div className="map-flight-airline">
            <span className="map-plane-icon">✈️</span>
            <div><strong>{flight.flight_id}</strong><small>{flight.airline}</small></div>
          </div>
          <div className="map-route-summary">
            <div><strong>{flight.origin}</strong><small>{from.name}</small></div>
            <div className="map-route-line"><span></span><Plane size={17}/><span></span></div>
            <div><strong>{flight.destination}</strong><small>{to.name}</small></div>
          </div>
          <div className="map-flight-status">
            <span className={`status ${flight.status === 'DELAYED' ? 'delayed' : 'pending'}`}>{flight.status}</span>
            <small>{flight.delay_minutes} min delay · Gate {flight.gate || '—'}</small>
          </div>
        </div>
      )}

      <div className="map-container">
        <MapContainer
          key={flight.flight_id}
          center={midpoint}
          zoom={5}
          scrollWheelZoom
          className="flight-map"
        >
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          <FitRoute from={from.coords} to={to.coords}/>
          <Polyline positions={[from.coords, to.coords]}
            pathOptions={{color:'#287cff', weight:4, dashArray:'9 9'}}/>
          <Marker position={from.coords} icon={airportIcon(flight.origin, '#287cff')}>
            <Popup>{from.name} airport · Origin</Popup>
          </Marker>
          <Marker position={to.coords} icon={airportIcon(flight.destination, '#e05b58')}>
            <Popup>{to.name} airport · Destination</Popup>
          </Marker>
          <Marker position={midpoint} icon={aircraftIcon}>
            <Popup>
              <strong>{flight.flight_id}</strong><br/>
              Illustrative route marker — not live GPS telemetry
            </Popup>
          </Marker>
        </MapContainer>
      </div>

      <div className="map-legend">
        <span><i className="legend-blue"/> Departure airport</span>
        <span><i className="legend-red"/> Arrival airport</span>
        <span>✈️ Illustrative route marker</span>
      </div>
      <p className="map-disclaimer">
        The aircraft marker shows the route midpoint, not the aircraft's actual position.
        Live GPS tracking requires latitude and longitude telemetry.
      </p>
    </section>
  )
}
