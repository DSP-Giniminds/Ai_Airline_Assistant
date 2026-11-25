import { useState, useEffect, useCallback, useMemo } from 'react'
import axios from 'axios'
import {
  Plane, Users, BrainCircuit, Hotel, ClipboardCheck,
  LayoutDashboard, Bell, Search, RefreshCw,
  CheckCircle2, XCircle, Menu, Ticket, TrendingUp
} from 'lucide-react'
import './App.css'
import FlightMap from './FlightMap'
import CustomerPortal from './CustomerPortal' 

const API = '/api'

const menu = [
  ['Overview', LayoutDashboard],
  ['Flight Monitoring', Plane],
  ['Passengers', Users],
  ['AI Recovery', BrainCircuit],
  ['Hotel Inventory', Hotel],
  ['Rebooking Approvals', ClipboardCheck],
  ['Customer Portal', Ticket]
]

const emptyData = {
  flights: [],
  passengers: [],
  recommendations: [],
  hotels: [],
  rebookings: []
}

function latestRequests(events) {
  const grouped = new Map()

  ;[...events].sort((a, b) => (b.event_ts || 0) - (a.event_ts || 0))
    .forEach(event => {
      const id = `${event.booking_id}|${event.passenger_id}|${event.action}`
      if (!grouped.has(id)) grouped.set(id, event)
    })

  return [...grouped.values()]
}

function App() {
  const [page, setPage] = useState('Overview')
  const [data, setData] = useState(emptyData)
  const [busy, setBusy] = useState('')
  const [message, setMessage] = useState('')
  const [mobileMenu, setMobileMenu] = useState(false)
  const [loading, setLoading] = useState(true)
  const [refreshing, setRefreshing] = useState(false)
  const [lastUpdated, setLastUpdated] = useState(null)
  const [apiErrors, setApiErrors] = useState([])

  const loadData = useCallback(async (manual = false) => {
    if (manual) setRefreshing(true)

    const endpoints = [
      ['flights', '/flights'],
      ['passengers', '/passengers'],
      ['recommendations', '/recommendations'],
      ['hotels', '/hotels'],
      ['rebookings', '/rebooking/requests']
    ]

    const results = await Promise.allSettled(
      endpoints.map(([, url]) => axios.get(`${API}${url}`, { timeout: 35000 }))
    )

    const next = {}
    const errors = []

    results.forEach((result, index) => {
      const [name, url] = endpoints[index]

      if (result.status === 'fulfilled') {
        next[name] = result.value.data?.data || []
      } else {
        errors.push(`${url}: ${result.reason?.response?.data?.detail || result.reason.message}`)
      }
    })

    setData(previous => ({ ...previous, ...next }))
    setApiErrors(errors)
    setLastUpdated(new Date())
    setLoading(false)
    setRefreshing(false)
  }, [])

  useEffect(() => {
    loadData()
    const timer = setInterval(() => loadData(), 10000)
    return () => clearInterval(timer)
  }, [loadData])

  const requests = useMemo(
    () => latestRequests(data.rebookings),
    [data.rebookings]
  )

  const pending = requests.filter(r => r.status === 'PENDING_APPROVAL').length
  const atRisk = data.passengers.filter(
    p => ['HIGH', 'CRITICAL'].includes((p.connection_risk || '').toUpperCase())
  ).length
  const delayedFlights = data.flights.filter(
    f => (f.status || '').toUpperCase() === 'DELAYED'
  ).length
  const availableRooms = data.hotels.reduce(
    (total, hotel) => total + (Number(hotel.available_rooms) || 0), 0
  )

  async function decide(item, decision) {
    const key = item._key?.request_id || `${item.booking_id}-${item.passenger_id}`
    setBusy(key)
    setMessage('')

    try {
      const endpoint = decision === 'APPROVED'
        ? '/rebooking/approve'
        : '/rebooking/reject'

      const response = await axios.post(`${API}${endpoint}`, {
        booking_id: item.booking_id,
        passenger_id: item.passenger_id,
        original_flight_id: item.original_flight_id,
        action: item.action,
        target_flight_id: item.target_flight_id,
        hotel_id: item.hotel_id,
        approved_by: 'operations-team'
      })

      setMessage(`${decision} event published. Request ID: ${response.data.request_id}`)
      await loadData()
    } catch (error) {
      setMessage(error.response?.data?.detail || error.message || 'API request failed')
    } finally {
      setBusy('')
    }
  }

  const formatTime = value => value
    ? new Date(value).toLocaleTimeString()
    : 'Not available'

  const passengerRows = data.passengers.map(p => ({
    id: p._key?.passenger_id || p.passenger_id || '—',
    booking: p.booking_id || '—',
    flight: p.flight_id || '—',
    connection: p.next_flight_id || '—',
    airport: p.connection_airport || '—',
    remaining: p.remaining_connection_minutes,
    risk: p.connection_risk || 'UNKNOWN'
  }))

  return (
    <div className="app-shell">
      <aside className={`sidebar ${mobileMenu ? 'mobile-open' : ''}`}>
        <div className="brand">
          <div className="brand-icon"><Plane size={23}/></div>
          <div><strong>Airline<span>IQ</span></strong><small>OPERATIONS CENTER</small></div>
        </div>

        <div className="nav-label">WORKSPACE</div>
        {menu.map(([name, Icon]) => (
          <button key={name}
            className={`nav-item ${page === name ? 'active' : ''}`}
            onClick={() => { setPage(name); setMobileMenu(false) }}>
            <Icon size={18}/><span>{name}</span>
          </button>
        ))}

        <div className="sidebar-bottom">
          <div className="connection">
            <span className="live-dot"/> Kafka API connected
          </div>
          <small>AirlineIQ · POC v1.0</small>
        </div>
      </aside>

      <main className="main-area">
        <header className="topbar">
          <button className="mobile-toggle" onClick={() => setMobileMenu(!mobileMenu)}><Menu/></button>
          <div className="breadcrumb">Operations <span>/</span> {page}</div>
          <div className="top-actions">
            <div className="system-status"><span className="live-dot"/> System online</div>
            <button className="icon-button"><Search size={19}/></button>
            <button className="icon-button"><Bell size={19}/></button>
            <div className="avatar">OP</div>
          </div>
        </header>

        <div className="content">
          <div className="page-heading">
            <div>
              <div className="eyebrow">AIRLINE OPERATIONS PLATFORM</div>
              <h1>{page}</h1>
              <p>Live operational data from Confluent Cloud and the Airline API.</p>
              <small className="muted">
                {lastUpdated ? `Last updated: ${formatTime(lastUpdated)}` : 'Loading live data…'}
              </small>
            </div>
            <button className="refresh-button" onClick={() => loadData(true)} disabled={refreshing}>
              <RefreshCw size={15}/> {refreshing ? 'Refreshing…' : 'Refresh'}
            </button>
          </div>

          {apiErrors.length > 0 && (
            <div className="notice">
              Some data sources could not be refreshed: {apiErrors.join(' | ')}
            </div>
          )}
          {message && <div className="notice">{message}</div>}
          {loading && <p className="muted">Loading live Kafka data…</p>}

          {page === 'Overview' && <>
            <div className="metrics">
              <Metric icon={Plane} label="Disrupted flights"
                value={String(delayedFlights).padStart(2, '0')}
                note={`${data.flights.length} flights in feed`}
                onClick={() => setPage('Flight Monitoring')}/>
              <Metric icon={Users} label="At-risk passengers"
                value={String(atRisk).padStart(2, '0')}
                note={`${data.passengers.length} passenger records`}
                onClick={() => setPage('Passengers')}/>
              <Metric icon={ClipboardCheck} label="Pending approvals"
                value={String(pending).padStart(2, '0')}
                note={`${data.rebookings.length} Kafka events`}
                onClick={() => setPage('Rebooking Approvals')}/>
              <Metric icon={Hotel} label="Hotel rooms available"
                value={String(availableRooms).padStart(2, '0')}
                note={`${data.hotels.length} hotels in feed`}
                onClick={() => setPage('Hotel Inventory')}/>
            </div>

            <FlightMap flights={data.flights}/>

            <div className="dashboard-grid">
              <section className="panel flight-panel">
                <SectionTitle title="Flight disruption" subtitle="Latest flight status from Kafka"/>
                {data.flights.length ? data.flights.map(f => (
                  <div key={f.flight_id}>
                    <div className="flight-number">{f.flight_id} <span>{f.airline}</span></div>
                    <div className="route">
                      <div><strong>{f.origin}</strong><small>Origin</small><span>{f.scheduled_arrival || '—'}</span></div>
                      <div className="route-line"><Plane size={17}/><div/></div>
                      <div><strong>{f.destination}</strong><small>Destination</small><span className="red-text">{f.estimated_arrival || '—'}</span></div>
                    </div>
                    <div className="flight-info">
                      <div><small>Status</small><strong>{f.status}</strong></div>
                      <div><small>Delay</small><strong>{f.delay_minutes} min</strong></div>
                      <div><small>Gate</small><strong>{f.gate || '—'}</strong></div>
                    </div>
                  </div>
                )) : <Empty text="No flight records received from Kafka."/>}
              </section>

              <section className="panel passenger-panel">
                <SectionTitle title="Passenger connection risk" subtitle={`${passengerRows.length} live passenger records`}/>
                <PassengerTable rows={passengerRows}/>
              </section>

              <section className="panel recovery-panel">
                <SectionTitle title="Latest AI recovery insights" subtitle="Recommendations from Kafka"/>
                {data.recommendations.slice(0, 3).map((r, i) => (
                  <div className="insight" key={`${r.booking_id}-${r.passenger_id}-${i}`}>
                    <div className="insight-icon"><BrainCircuit size={20}/></div>
                    <div>
                      <strong>{r.booking_id} · {r.passenger_id}</strong>
                      <p>{r.ai_recommendation}</p>
                    </div>
                  </div>
                ))}
                {!data.recommendations.length && <Empty text="No AI recommendations available."/>}
              </section>
            </div>
          </>}

          {page === 'Customer Portal' && <CustomerPortal/>}

          {page === 'Flight Monitoring' && <section className="panel">
            <SectionTitle title="Live flight status" subtitle="Records from flight_status"/>
            <DataTable headers={['Flight','Airline','Origin','Destination','Status','Delay','Gate']}
              rows={data.flights.map(f => [f.flight_id, f.airline, f.origin, f.destination, f.status, `${f.delay_minutes} min`, f.gate])}/>
            {!data.flights.length && <Empty text="No flight data available."/>}
          </section>}

          {page === 'Passengers' && <section className="panel">
            <SectionTitle title="Passenger connection risk" subtitle="Records from passenger_state_stream"/>
            <PassengerTable rows={passengerRows} full/>
          </section>}

          {page === 'AI Recovery' && <section className="panel">
            <SectionTitle title="AI recovery recommendations" subtitle="Records from ai_recovery_recommendations"/>
            {data.recommendations.map((r, i) => (
              <div className="recommendation-card" key={`${r.booking_id}-${r.passenger_id}-${i}`}>
                <BrainCircuit size={26}/>
                <div>
                  <h3>{r.flight_id} · {r.booking_id} · {r.passenger_id}</h3>
                  <p>{r.ai_recommendation}</p>
                  <span className="tag">{r.event_type} · RECOMMENDATION ONLY</span>
                </div>
              </div>
            ))}
            {!data.recommendations.length && <Empty text="No recommendations received."/>}
          </section>}

          {page === 'Hotel Inventory' && <section className="panel">
            <SectionTitle title="Live hotel inventory" subtitle="Latest records from hotel_inventory"/>
            {data.hotels.map(h => (
              <div className="hotel-card" key={h.hotel_id}>
                <Hotel size={28}/>
                <div>
                  <h3>{h.hotel_name}</h3>
                  <p>{h.city} · {h.hotel_id}</p>
                  <strong>{h.available_rooms} rooms available · {h.currency} {Number(h.price_per_night).toLocaleString()} / night</strong>
                </div>
                <span className={`status ${h.available_rooms > 0 ? 'available' : 'high'}`}>
                  {h.available_rooms > 0 ? 'AVAILABLE' : 'SOLD OUT'}
                </span>
              </div>
            ))}
            {!data.hotels.length && <Empty text="No hotel inventory records received."/>}
          </section>}

          {page === 'Rebooking Approvals' && <section className="panel">
            <SectionTitle title="Rebooking approval queue" subtitle="Latest event state per booking, passenger and action"/>
            {requests.map(item => {
              const key = item._key?.request_id || `${item.booking_id}-${item.passenger_id}`
              return (
                <div className="approval-card" key={key}>
                  <div className="approval-main">
                    <div className="approval-icon"><ClipboardCheck size={21}/></div>
                    <div>
                      <h3>{item.booking_id} · {item.passenger_id}</h3>
                      <p>{item.original_flight_id} → {item.target_flight_id || 'Not specified'} · {item.action}</p>
                      <small>{item.approved_by ? `Approver: ${item.approved_by}` : 'Awaiting approval'}</small>
                    </div>
                    <span className={`status ${item.status === 'APPROVED' ? 'available' : item.status === 'REJECTED' ? 'high' : 'pending'}`}>
                      {item.status}
                    </span>
                  </div>
                  {item.status === 'PENDING_APPROVAL' && (
                    <div className="approval-actions">
                      <button className="approve-button" disabled={!!busy} onClick={() => decide(item, 'APPROVED')}>
                        <CheckCircle2 size={16}/> {busy === key ? 'Sending…' : 'Approve'}
                      </button>
                      <button className="reject-button" disabled={!!busy} onClick={() => decide(item, 'REJECTED')}>
                        <XCircle size={16}/> Reject
                      </button>
                    </div>
                  )}
                </div>
              )
            })}
            {!requests.length && <Empty text="No rebooking events received from Kafka."/>}
            <p className="muted">Approval and rejection actions publish events. They do not confirm an actual airline reservation.</p>
          </section>}

          <footer>AirlineIQ Operations · Confluent Cloud · Flink · FastAPI · React</footer>
        </div>
      </main>
    </div>
  )
}

function Metric({icon: Icon, label, value, note, onClick}) {
  return (
    <button type="button" className="metric-card metric-button" onClick={onClick}>
      <div className="metric-top"><span>{label}</span><Icon size={19}/></div>
      <div className="metric-value">{value}</div>
      <div className="metric-note"><TrendingUp size={13}/>{note}</div>
    </button>
  )
}

function SectionTitle({title, subtitle}) {
  return <div className="panel-heading"><div><h3>{title}</h3><p>{subtitle}</p></div></div>
}

function PassengerTable({rows, full = false}) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Passenger</th><th>Booking</th><th>Flight</th>
            {full && <><th>Next flight</th><th>Airport</th></>}
            <th>Remaining</th><th>Risk</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(p => (
            <tr key={`${p.id}-${p.booking}`}>
              <td>{p.id}</td><td>{p.booking}</td><td>{p.flight}</td>
              {full && <><td>{p.connection}</td><td>{p.airport}</td></>}
              <td>{p.remaining ?? '—'}{p.remaining != null ? ' min' : ''}</td>
              <td><span className={`status ${['HIGH','CRITICAL'].includes(p.risk.toUpperCase()) ? 'high' : 'pending'}`}>{p.risk}</span></td>
            </tr>
          ))}
        </tbody>
      </table>
      {!rows.length && <Empty text="No passenger records received."/>}
    </div>
  )
}

function DataTable({headers, rows}) {
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>{headers.map(h => <th key={h}>{h}</th>)}</tr></thead>
        <tbody>{rows.map((row, i) => <tr key={i}>{row.map((v, j) => <td key={j}>{v ?? '—'}</td>)}</tr>)}</tbody>
      </table>
      {!rows.length && <Empty text="No records received."/>}
    </div>
  )
}

function Empty({text}) {
  return <p className="muted" style={{padding: '14px 0'}}>{text}</p>
}

export default App
