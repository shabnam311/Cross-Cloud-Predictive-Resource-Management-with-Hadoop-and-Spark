const MONTH_NAMES = [
  'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'
];

export function formatPercent(value, decimals = 0) {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  const num = typeof value === 'number' ? value * 100 : parseFloat(value) * 100;
  return `${num.toFixed(decimals)}%`;
}

export function formatNumber(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return new Intl.NumberFormat('en-GB').format(value);
}

export function formatDecimal(value, decimals = 3) {
  if (value === null || value === undefined || Number.isNaN(value)) return '—';
  return Number(value).toFixed(decimals);
}

export function formatDate(isoString) {
  if (!isoString) return '—';
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) return isoString;

  const day = date.getUTCDate();
  const month = MONTH_NAMES[date.getUTCMonth()];
  const year = date.getUTCFullYear();
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');

  // Format: "4 Oct 2026, 14:00"
  return `${day} ${month} ${year}, ${hours}:${minutes}`;
}

export function formatTime(isoString) {
  if (!isoString) return '';
  const date = new Date(isoString);
  if (Number.isNaN(date.getTime())) return isoString;
  const hours = String(date.getUTCHours()).padStart(2, '0');
  const minutes = String(date.getUTCMinutes()).padStart(2, '0');
  return `${hours}:${minutes}`;
}
