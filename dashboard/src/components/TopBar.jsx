import React from 'react';
import { NavLink } from 'react-router-dom';

const navItems = [
  { path: '/', label: 'Overview' },
  { path: '/forecasts', label: 'Forecasts' },
  { path: '/cross-cloud', label: 'Cross-cloud' },
  { path: '/recommendations', label: 'Recommendations' },
  { path: '/models', label: 'Models' },
  { path: '/pipeline', label: 'Pipeline' },
  { path: '/about', label: 'About' },
];

export function TopBar() {
  return (
    <header className="top-bar">
      <div className="top-bar-inner">
        <NavLink to="/" className="top-bar-brand">
          Cross-Cloud Resource Forecasting
        </NavLink>
        <nav className="top-bar-nav" aria-label="Main Navigation">
          {navItems.map((item) => (
            <NavLink
              key={item.path}
              to={item.path}
              end={item.path === '/'}
              className={({ isActive }) =>
                isActive ? 'top-bar-link active' : 'top-bar-link'
              }
            >
              {item.label}
            </NavLink>
          ))}
        </nav>
      </div>
    </header>
  );
}
