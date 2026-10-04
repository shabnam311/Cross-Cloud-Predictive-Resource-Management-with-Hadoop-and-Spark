import React from 'react';
import { Routes, Route, Navigate, Link } from 'react-router-dom';
import { TopBar } from './components/TopBar';
import { Footer } from './components/Footer';
import { useData } from './hooks/useData';

import { Overview } from './pages/Overview';
import { Forecasts } from './pages/Forecasts';
import { CrossCloud } from './pages/CrossCloud';
import { Recommendations } from './pages/Recommendations';
import { Models } from './pages/Models';
import { Pipeline } from './pages/Pipeline';
import { About } from './pages/About';

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
      </main>
      <Footer generatedAt={overview?.generatedAt} isSample={overview?.isSample ?? true} />
    </div>
  );
}

export default App;
