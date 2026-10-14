import React from 'react';
import { formatDate } from '../lib/format';

export function Footer({ generatedAt, isSample }) {
  const statusText = isSample ? 'Sample data' : 'Pipeline output';
  const dateText = generatedAt ? formatDate(generatedAt) : '4 Oct 2026, 12:00';

  return (
    <footer className="site-footer">
      <div className="footer-inner">
        <span>Cross-Cloud Predictive Resource Management with Hadoop and Spark</span>
        <span>
          {statusText}. Generated {dateText}.
        </span>
      </div>
    </footer>
  );
}
