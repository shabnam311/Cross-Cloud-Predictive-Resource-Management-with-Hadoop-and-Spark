import React from 'react';
import { SampleNotice } from './SampleNotice';

export function PageHeader({ title, description, isSample }) {
  return (
    <div className="page-header">
      <h1 className="page-title">{title}</h1>
      {description && <p className="page-description">{description}</p>}
      {isSample && <SampleNotice />}
    </div>
  );
}
