/**
 * Shared formatting utilities for the lakehouse admin frontend.
 */

/**
 * Formats a number with locale-specific thousand separators.
 * @param num - The number to format
 * @returns Formatted number string (e.g., "1,234,567")
 */
export function formatNumber(num: number): string {
  return num.toLocaleString('en-US');
}

/**
 * Formats bytes into human-readable file size.
 * @param bytes - The number of bytes
 * @returns Formatted size string (e.g., "1.50 GB")
 */
export function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 Bytes';

  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}

/**
 * Formats an ISO timestamp into RFC 3339 format.
 * @param timestamp - ISO 8601 timestamp string
 * @returns RFC 3339 formatted timestamp (e.g., "2025-11-21T10:30:45Z")
 */
export function formatTimestamp(timestamp: string): string {
  const date = new Date(timestamp);
  return date.toISOString();
}
