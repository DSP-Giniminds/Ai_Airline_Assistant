import React from 'react'
import { createRoot } from 'react-dom/client'
import App from './App.jsx'
import CustomerPortal from './CustomerPortal.jsx'
import './index.css'

const isPassengerPortal = window.location.pathname.startsWith('/passenger')

createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    {isPassengerPortal ? <CustomerPortal /> : <App />}
  </React.StrictMode>
)
