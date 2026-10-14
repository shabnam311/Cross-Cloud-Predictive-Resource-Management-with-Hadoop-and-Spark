import React from 'react';

export function Figure({ caption, ariaLabel, children }) {
  return (
    <figure className="figure-container" aria-label={ariaLabel}>
      {children}
      {caption && <figcaption className="figure-caption">{caption}</figcaption>}
    </figure>
  );
}
