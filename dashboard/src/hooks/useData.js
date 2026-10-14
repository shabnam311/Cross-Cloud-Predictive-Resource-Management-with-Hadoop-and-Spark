import { useState, useEffect } from 'react';

const cache = new Map();

export function useData(filename) {
  const [state, setState] = useState(() => {
    if (cache.has(filename)) {
      return { data: cache.get(filename), loading: false, error: null };
    }
    return { data: null, loading: true, error: null };
  });

  useEffect(() => {
    if (!filename) return;

    if (cache.has(filename)) {
      setState({ data: cache.get(filename), loading: false, error: null });
      return;
    }

    let isMounted = true;
    setState((prev) => ({ ...prev, loading: true, error: null }));

    const baseUrl = import.meta.env.BASE_URL.endsWith('/')
      ? import.meta.env.BASE_URL
      : `${import.meta.env.BASE_URL}/`;
    const url = `${baseUrl}data/${filename}`;

    fetch(url)
      .then((res) => {
        if (!res.ok) {
          throw new Error(`Could not load ${filename}. Check that the file exists in dashboard/public/data.`);
        }
        return res.json();
      })
      .then((json) => {
        cache.set(filename, json);
        if (isMounted) {
          setState({ data: json, loading: false, error: null });
        }
      })
      .catch((err) => {
        if (isMounted) {
          setState({
            data: null,
            loading: false,
            error: err.message || `Could not load ${filename}. Check that the file exists in dashboard/public/data.`,
          });
        }
      });

    return () => {
      isMounted = false;
    };
  }, [filename]);

  return state;
}
