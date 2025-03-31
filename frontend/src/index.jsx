import React from 'react';
import ReactDOM from 'react-dom/client';
import './styles/index.css'; // Ensure Tailwind styles are imported
import App from './App';
import { Route } from 'react-router-dom';
// import { BrowserRouter, Routes } from 'react-router-dom'
import Documentation from './components/Documentation';

// In your Routes component
<Route path="/docs" element={<Documentation/>} />

const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);