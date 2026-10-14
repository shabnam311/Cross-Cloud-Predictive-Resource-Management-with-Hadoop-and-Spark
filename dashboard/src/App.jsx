import React, { lazy, Suspense } from 'react';
import { Routes, Route, Navigate, Link } from 'react-router-dom';
import { TopBar } from './components/TopBar';
import { Footer } from './components/Footer';
import { useData } from './hooks/useData';

const Overview = lazy(() => import('./pages/Overview').then((m) => ({ default: m.Overview })));
const Forecasts = lazy(() => import('./pages/Forecasts').then((m) => ({ default: m.Forecasts })));
const CrossCloud = lazy(() => import('./pages/CrossCloud').then((m) => ({ default: m.CrossCloud })));
const Recommendations = lazy(() => import('./pages/Recommendations').then((m) => ({ default: m.Recommendations })));
const Models = lazy(() => import('./pages/Models').then((m) => ({ default: m.Models })));
const Pipeline = lazy(() => import('./pages/Pipeline').then((m) => ({ default: m.Pipeline })));
const About = lazy(() => import('./pages/About').then((m) => ({ default: m.About })));

function NotFound() {
  return (
    <div className="page-section">
      <h1 className="page-title">Page not found</h1>
      <p className="page-description">
        The requested page does not exist. Return to the <Link to="/">overview page</Link>.
      </p>
    </div>
  );
}

export function App() {
  const { data: overview } = useData('overview.json');

  return (
    <div className="app-container">
      <TopBar />
      <main className="main-content">
        <Suspense fallback={<div className="state-container"><div className="state-title">Loading...</div></div>}>
          <Routes>
            <Route path="/" element={<Overview />} />
            <Route path="/forecasts" element={<Forecasts />} />
            <Route path="/cross-cloud" element={<CrossCloud />} />
            <Route path="/recommendations" element={<Recommendations />} />
            <Route path="/models" element={<Models />} />
            <Route path="/pipeline" element={<Pipeline />} />
            <Route path="/about" element={<About />} />
            <Route path="/overview" element={<Navigate to="/" replace />} />
            <Route path="*" element={<NotFound />} />
          </Routes>
        </Suspense>
      </main>
      <Footer generatedAt={overview?.generatedAt} isSample={overview?.isSample ?? true} />
    </div>
  );
}

export default App;
