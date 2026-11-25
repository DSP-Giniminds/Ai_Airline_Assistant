
import { useState } from 'react'
import axios from 'axios'
import HotelResults from './HotelResults'
import {
  Plane, Search, MapPin, Clock, ShieldCheck,
  Hotel, ArrowRight, Headphones, Ticket, RefreshCw,
  Armchair, CheckCircle, IndianRupee
} from 'lucide-react'
import './CustomerPortal.css'

export default function CustomerPortal() {
  const [booking, setBooking] = useState('')
  const [passengerId, setPassengerId] = useState('')
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [requesting, setRequesting] = useState(false)
  const [message, setMessage] = useState('')
  const [error, setError] = useState('')

  // AI CHAT STATE
  const [chatMessages, setChatMessages] = useState([])
  const [chatInput, setChatInput] = useState('')
  const [chatLoading, setChatLoading] = useState(false)

  // SELF-SERVICE FLIGHT BOOKING
  const [alternativeFlights, setAlternativeFlights] = useState([])
  const [flightsLoading, setFlightsLoading] = useState(false)
  const [selectedFlight, setSelectedFlight] = useState(null)

  const [seats, setSeats] = useState([])
  const [seatsLoading, setSeatsLoading] = useState(false)
  const [selectedSeat, setSelectedSeat] = useState('')

  const [bookingLoading, setBookingLoading] = useState(false)
  const [confirmedBooking, setConfirmedBooking] = useState(null)
  const [bookingError, setBookingError] = useState('')

  const [chatSeatSelection, setChatSeatSelection] = useState({})


  function selectChatSeat(messageIndex, seatNumber) {
    setChatSeatSelection(prev => ({
      ...prev,
      [messageIndex]: seatNumber
    }))
  }

  async function sendSeatSelection(seatNumber) {
    setChatInput(seatNumber)

    // Submit the selected seat as a normal chat message.
    const question = seatNumber

    setChatMessages(prev => [
      ...prev,
      { role: 'user', content: question }
    ])

    setChatLoading(true)

    try {
      const response = await axios.post('/api/chat/agent', {
        booking_id: booking.trim(),
        passenger_id: passengerId.trim(),
        message: question
      }, { timeout: 90000 })

      setChatMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: response.data.reply,
          intent: response.data.intent,
          flights: response.data.flights || [],
          seats: response.data.seats || [],
          booking: response.data.booking || null,
          hotelBooking: response.data.hotel_booking || null,
          hotels: response.data.hotels || [],
          hotelDiscovery: response.data.hotel_discovery || null,
          flight: response.data.flight || null,
          seatNumber: response.data.seat_number || null
        }
      ])

      setChatInput('')
    } catch (err) {
      setChatMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: err.response?.data?.detail || 'Unable to process seat selection.'
        }
      ])
    } finally {
      setChatLoading(false)
    }
  }
  async function sendChatMessage(question) {
    if (!question || chatLoading) return

    setChatMessages(prev => [
      ...prev,
      { role: 'user', content: question }
    ])

    setChatLoading(true)

    try {
      const response = await axios.post('/api/chat/agent', {
        booking_id: booking.trim(),
        passenger_id: passengerId.trim(),
        message: question
      }, { timeout: 90000 })

      setChatMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: response.data.reply,
          intent: response.data.intent,
          flights: response.data.flights || [],
          seats: response.data.seats || [],
          booking: response.data.booking || null,
          hotels: response.data.hotels || [],
          hotelDiscovery: response.data.hotel_discovery || null
        }
      ])
    } catch (err) {
      setChatMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: err.response?.data?.detail || 'Unable to process your request.'
        }
      ])
    } finally {
      setChatLoading(false)
    }
  }
  async function findBooking(e) {
    e.preventDefault()
    setLoading(true)
    setError('')
    setMessage('')
    setData(null)
    setChatMessages([])

    try {
      const response = await axios.get('/api/passenger/booking', {
        params: {
          booking_id: booking.trim(),
          passenger_id: passengerId.trim()
        },
        timeout: 60000
      })
      setData(response.data)
    } catch (e) {
      setError(e.response?.data?.detail || e.message || 'Unable to find booking.')
    } finally {
      setLoading(false)
    }
  }

  async function requestRecovery(action, hotelId = null) {
    if (!data?.passenger || !data?.flight) return

    setRequesting(true)
    setMessage('')
    setError('')

    try {
      const result = await axios.post('/api/rebooking/request', {
        booking_id: data.passenger.booking_id,
        passenger_id: data.passenger._key?.passenger_id || passengerId,
        original_flight_id: data.passenger.flight_id,
        action,
        target_flight_id: null,
        hotel_id: action === 'REBOOK' ? null : hotelId,
        approved_by: 'customer-portal'
      })

      setMessage(
        `Your request was submitted successfully. Reference: ${result.data.request_id}. It is awaiting airline operations review.`
      )

      const refreshed = await axios.get('/api/passenger/booking', {
        params: {
          booking_id: booking.trim(),
          passenger_id: passengerId.trim()
        },
        timeout: 60000
      })

      setData(refreshed.data)
    } catch (e) {
      setError(e.response?.data?.detail || e.message || 'Unable to submit request.')
    } finally {
      setRequesting(false)
    }
  }

  // GRANITE AI CHAT
  async function sendChat(e) {
    e.preventDefault()

    const question = chatInput.trim()

    if (!question || !data?.passenger || chatLoading) return

    setChatMessages(prev => [
      ...prev,
      { role: 'user', content: question }
    ])

    setChatInput('')
    setChatLoading(true)
    setError('')

    try {
      const response = await axios.post('/api/chat/agent', {
        booking_id: booking.trim(),
        passenger_id: passengerId.trim(),
        message: question
      }, {
        timeout: 90000
      })

      setChatMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: response.data.reply,
          intent: response.data.intent,
          flights: response.data.flights || [],
          seats: response.data.seats || [],
          booking: response.data.booking || null,
          hotels: response.data.hotels || [],
          hotelDiscovery: response.data.hotel_discovery || null,
          flight: response.data.flight || null,
          seatNumber: response.data.seat_number || null
        }
      ])
    } catch (err) {
      const detail = err.response?.data?.detail

      let errorMessage = 'Unable to reach the AI assistant. Please try again.'

      if (err.response?.status === 409 && typeof detail === 'object') {
        errorMessage = detail.existing_status === 'PENDING_APPROVAL'
          ? 'Your rebooking request is already pending approval. Please wait for the airline operations team.'
          : `This request already exists with status: ${detail.existing_status || 'UNKNOWN'}.`
      } else if (typeof detail === 'string') {
        errorMessage = detail
      } else if (typeof detail === 'object' && detail?.message) {
        errorMessage = detail.message
      }

      setChatMessages(prev => [
        ...prev,
        {
          role: 'assistant',
          content: errorMessage
        }
      ])
    } finally {
      setChatLoading(false)
    }
  }
  // LOAD ALTERNATIVE FLIGHTS
  async function loadAlternativeFlights() {
    if (!data?.passenger || !data?.flight) return

    setFlightsLoading(true)
    setBookingError('')
    setAlternativeFlights([])
    setSelectedFlight(null)
    setSeats([])
    setSelectedSeat('')
    setConfirmedBooking(null)

    try {
      const response = await axios.get('/api/alternative-flights', {
        params: {
          origin: data.flight.origin || 'BOM',
          destination: data.flight.destination || 'DEL',
          min_seats: 1
        },
        timeout: 60000
      })

      setAlternativeFlights(response.data.data || [])
    } catch (err) {
      setBookingError(
        err.response?.data?.detail || 'Unable to load alternative flights.'
      )
    } finally {
      setFlightsLoading(false)
    }
  }

  // SELECT FLIGHT AND LOAD SEATS
  async function selectFlight(flight) {
    setSelectedFlight(flight)
    setSeats([])
    setSelectedSeat('')
    setBookingError('')
    setConfirmedBooking(null)
    setSeatsLoading(true)

    try {
      const response = await axios.get(
        `/api/booking/seats/${encodeURIComponent(flight.flight_id)}`,
        { timeout: 60000 }
      )

      setSeats(response.data.seats || [])
    } catch (err) {
      setBookingError(
        err.response?.data?.detail || 'Unable to load seat availability.'
      )
    } finally {
      setSeatsLoading(false)
    }
  }

  // CONFIRM SELF-SERVICE BOOKING
  async function confirmSelfServiceBooking() {
    if (!selectedFlight || !selectedSeat || !p) return

    setBookingLoading(true)
    setBookingError('')
    setConfirmedBooking(null)

    try {
      const response = await axios.post('/api/booking/confirm', {
        booking_id: p.booking_id,
        passenger_id: p._key?.passenger_id || passengerId,
        original_flight_id: p.flight_id,
        new_flight_id: selectedFlight.flight_id,
        seat_number: selectedSeat
      }, {
        timeout: 60000
      })

      setConfirmedBooking(response.data)

      // Refresh seat availability
      const seatResponse = await axios.get(
        `/api/booking/seats/${encodeURIComponent(selectedFlight.flight_id)}`
      )

      setSeats(seatResponse.data.seats || [])
      setSelectedSeat('')
    } catch (err) {
      setBookingError(
        err.response?.data?.detail || 'Unable to confirm your booking.'
      )
    } finally {
      setBookingLoading(false)
    }
  }
  const p = data?.passenger
  const f = data?.flight
  const risk = (p?.connection_risk || 'UNKNOWN').toUpperCase()

  return (
    <div className="customer-page">

      <header className="customer-header">
        <div className="customer-brand">
          <span><Plane size={22}/></span>
          <b>AirlineIQ</b>
        </div>
        <div className="customer-help">
          <Headphones size={17}/> Passenger assistance
        </div>
      </header>

      <section className="customer-hero">
        <div className="customer-eyebrow">YOUR JOURNEY, OUR PRIORITY</div>
        <h1>Travel with confidence.</h1>
        <p>
          View your flight information and request assistance
          when your travel plans change.
        </p>

        <form className="booking-search" onSubmit={findBooking}>
          <label>Booking reference</label>

          <div className="booking-inputs">
            <input
              value={booking}
              onChange={e => setBooking(e.target.value)}
              placeholder="Enter booking reference"
              required
            />

            <input
              value={passengerId}
              onChange={e => setPassengerId(e.target.value)}
              placeholder="Passenger ID"
              required
            />

            <button type="submit" disabled={loading}>
              <Search size={17}/>
              {loading ? 'Searching…' : 'Find booking'}
            </button>
          </div>

          <small>
            Enter the booking reference and passenger ID provided by your airline.
          </small>
        </form>
      </section>

      {error && <div className="customer-message">{error}</div>}
      {message && <div className="customer-message">{message}</div>}

      {data && p && (
        <section className="customer-results">

          {/* FLIGHT DETAILS */}

          <div className="customer-section-heading">
            <div>
              <span className="customer-eyebrow">BOOKING DETAILS</span>
              <h2>Your flight information</h2>
            </div>

            <span className={`customer-delay ${f?.status === 'DELAYED' ? '' : 'status'}`}>
              {f?.status || 'STATUS UNAVAILABLE'}
            </span>
          </div>

          <div className="customer-flight-card">
            <div className="customer-flight-top">
              <span><Ticket size={16}/> Booking {p.booking_id}</span>
              <b>{p.flight_id} · {f?.airline || 'Airline unavailable'}</b>
            </div>

            <div className="customer-route">
              <div>
                <strong>{f?.origin || '—'}</strong>
                <span>{f?.origin || 'Origin'}</span>
                <small>Scheduled arrival: {f?.scheduled_arrival || '—'}</small>
              </div>

              <div className="customer-route-line">
                <Plane size={20}/>
                <div/>
              </div>

              <div>
                <strong>{f?.destination || '—'}</strong>
                <span>{f?.destination || 'Destination'}</span>
                <small>Estimated arrival: {f?.estimated_arrival || '—'}</small>
              </div>
            </div>

            {f && (
              <div className="customer-alert">
                <Clock size={19}/>
                <div>
                  <b>
                    {f.status === 'DELAYED'
                      ? `Your flight is delayed by ${f.delay_minutes} minutes`
                      : `Current flight status: ${f.status}`}
                  </b>
                  <span>Gate: {f.gate || 'Not available'}</span>
                </div>
              </div>
            )}
          </div>

          {/* CONNECTION STATUS */}

          <div className="customer-flight-card">
            <div className="customer-section-heading">
              <div>
                <span className="customer-eyebrow">CONNECTION STATUS</span>
                <h2>Your onward journey</h2>
              </div>

              <span className={`customer-delay ${risk === 'HIGH' || risk === 'CRITICAL' ? '' : 'status'}`}>
                {risk} RISK
              </span>
            </div>

            <div className="customer-alert">
              <ShieldCheck size={20}/>
              <div>
                <b>Connecting flight: {p.next_flight_id || 'Not available'}</b>
                <span>Connection airport: {p.connection_airport || '—'}</span>
                <span>
                  Remaining connection time: {p.remaining_connection_minutes ?? '—'} minutes
                </span>
              </div>
            </div>

          </div>
          {/* AI PASSENGER CHAT */}

          <div className="customer-chat-card">

            <div className="customer-chat-header">
              <div>
                <span className="customer-eyebrow">AIRLINEIQ AI</span>
                <h2>
                  <Headphones size={21}/>
                  Passenger AI Assistant
                </h2>
                <p>
                  Ask questions about your flight and recovery options.
                </p>
              </div>

              <span className="customer-ai-status">AI Online</span>
            </div>

            <div className="customer-chat-messages">

              {chatMessages.length === 0 && (
                <div className="customer-chat-welcome">
                  <Headphones size={30}/>
                  <h3>Hello! How can I assist you?</h3>
                  <p>
                    Ask me about your flight delay, connection risk,
                    rebooking or hotel availability.
                  </p>
                </div>
              )}

              {chatMessages.map((msg, index) => (
                <div
                  key={index}
                  className={`customer-chat-message ${msg.role}`}
                >
                  <div className="customer-chat-bubble">
                    <div style={{ whiteSpace: 'pre-line' }}>
                      {msg.content}
                    </div>

                    {msg.hotels?.length > 0 && (
                      <HotelResults
                        hotels={msg.hotels}
                        discovery={msg.hotelDiscovery}
                      />
                    )}

                    {msg.seats?.length > 0 && (
                      <div className="chat-seat-options">
                        <div className="chat-seat-grid">
                          {msg.seats.map(seat => (
                            <button
                              type="button"
                              key={seat.seat_number}
                              onClick={() => selectChatSeat(index, seat.seat_number)}
                              className={
                                chatSeatSelection[index] === seat.seat_number
                                  ? 'chat-seat selected'
                                  : 'chat-seat'
                              }
                            >
                              {seat.seat_number}
                            </button>
                          ))}
                        </div>

                        <button
                          type="button"
                          disabled={!chatSeatSelection[index] || chatLoading}
                          onClick={() => sendSeatSelection(chatSeatSelection[index])}
                        >
                          Confirm seat selection
                        </button>
                      </div>
                    )}

                    {msg.hotelBooking && (
                      <div className="chat-booking-confirmation">
                        <b>Simulated hotel reservation confirmed</b>
                        <p>Booking reference: {msg.hotelBooking.booking_ref}</p>
                        <p>Hotel: {msg.hotelBooking.hotel_name}</p>
                        <p>Room: {msg.hotelBooking.room_type}</p>
                        <p>Check-in: {msg.hotelBooking.check_in}</p>
                        <p>Check-out: {msg.hotelBooking.check_out}</p>
                        <p>Nights: {msg.hotelBooking.nights}</p>
                        <p>Rooms: {msg.hotelBooking.rooms}</p>
                        <p>Guests: {msg.hotelBooking.guests}</p>
                        <p>
                          Price per night: {msg.hotelBooking.currency}{' '}
                          {Number(msg.hotelBooking.price_per_night).toLocaleString('en-IN')}
                        </p>
                        <p>
                          Total: {msg.hotelBooking.currency}{' '}
                          {Number(msg.hotelBooking.total_amount).toLocaleString('en-IN')}
                        </p>
                        <p>Status: {msg.hotelBooking.booking_status}</p>
                        <small>
                          This is a simulated reservation. No real hotel booking has been made.
                        </small>
                      </div>
                    )}

                    {msg.booking && (
                      <div className="chat-booking-confirmation">
                        <b>
                          {msg.booking.booking_status === 'CANCELLED'
                            ? 'Booking cancelled'
                            : 'Booking confirmed'}
                        </b>
                        <p>PNR: {msg.booking.pnr}</p>
                        <p>Flight: {msg.booking.flight_id || msg.booking.new_flight_id}</p>
                        <p>Seat: {msg.booking.seat_number}</p>
                        <p>
                          Amount: {msg.booking.currency}{' '}
                          {Number(msg.booking.amount).toLocaleString('en-IN')}
                        </p>
                        {msg.booking.booking_status === 'CANCELLED' && (
                          <p>This ticket is cancelled. The seat has been released.</p>
                        )}
                      </div>
                    )}
                  </div>
                </div>
              ))}
              {chatLoading && (
                <div className="customer-chat-message assistant">
                  <div className="customer-chat-bubble">
                    Granite is checking your travel information...
                  </div>
                </div>
              )}

            </div>

            <form className="customer-chat-input" onSubmit={sendChat}>
              <input
                value={chatInput}
                onChange={e => setChatInput(e.target.value)}
                placeholder="Ask your airline AI assistant..."
                disabled={chatLoading}
                maxLength={1000}
              />

              <button
                type="submit"
                disabled={chatLoading || !chatInput.trim()}
              >
                <ArrowRight size={19}/>
              </button>
            </form>

            <small className="customer-chat-note">
              Responses are generated using your live booking information.
            </small>

          </div>

          {/* RECOVERY ASSISTANCE */}

          <div className="customer-section-heading">
            <div>
              <span className="customer-eyebrow">RECOVERY ASSISTANCE</span>
              <h2>How can we help?</h2>
            </div>
          </div>

          <div className="customer-options">

            <div className="customer-option">
              <Plane size={23}/>
              <h3>Request rebooking</h3>
              <p>
                Ask the airline team to review alternative flight options.
              </p>

              <button
                disabled={requesting || !f}
                onClick={() => requestRecovery('REBOOK')}
              >
                Request rebooking <ArrowRight size={16}/>
              </button>
            </div>

            <div className="customer-option">
              <Hotel size={23}/>
              <h3>Request hotel assistance</h3>
              <p>
                Ask the airline team to review accommodation options.
              </p>

              {data.hotels?.map(h => (
                <div key={h.hotel_id}>
                  <p>{h.hotel_name} · {h.city}</p>
                  <p>
                    {h.available_rooms} rooms · {h.currency}{' '}
                    {Number(h.price_per_night).toLocaleString()} / night
                  </p>

                  <button
                    disabled={requesting || h.available_rooms <= 0}
                    onClick={() => requestRecovery('HOTEL', h.hotel_id)}
                  >
                    Request this hotel <ArrowRight size={16}/>
                  </button>
                </div>
              ))}

              {!data.hotels?.length && (
                <p>No hotel inventory currently available.</p>
              )}
            </div>

          </div>

          {/* REQUEST HISTORY */}

          <div className="customer-section-heading">
            <div>
              <span className="customer-eyebrow">REQUEST HISTORY</span>
              <h2>Your assistance requests</h2>
            </div>

            <button
              className="refresh-button"
              onClick={findBooking}
              disabled={loading}
            >
              <RefreshCw size={14}/> Refresh
            </button>
          </div>

          {(data.rebooking_events || []).length ? (
            data.rebooking_events.map((r, i) => (
              <div
                className="customer-flight-card"
                key={r._key?.request_id || i}
              >
                <div className="customer-flight-top">
                  <span>{r.action} · {r.original_flight_id}</span>
                  <b>{r.status}</b>
                </div>

                <p>Reference: {r._key?.request_id || '—'}</p>

                <small>
                  {r.approved_by
                    ? `Reviewed by ${r.approved_by}`
                    : 'Awaiting operations review'}
                </small>
              </div>
            ))
          ) : (
            <p>No assistance requests found for this booking.</p>
          )}

        </section>
      )}

      <footer className="customer-footer">
        <MapPin size={14}/> AirlineIQ Passenger Services
      </footer>

    </div>
  )
}
