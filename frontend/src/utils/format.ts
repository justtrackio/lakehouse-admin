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

/**
 * Formats an ISO timestamp into a human-readable date time string.
 * @param timestamp - ISO 8601 timestamp string
 * @returns Formatted date time string (e.g., "2026-02-04 19:07:55")
 */
export function formatDateTime(timestamp: string): string {
  const date = new Date(timestamp);
  return date.toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
}

/**
 * Formats a duration in milliseconds using abbreviated units.
 * @param durationMs - The duration in milliseconds
 * @returns Formatted duration string (e.g., "1.54s", "6.09m", "1.25h")
 */
export function formatDuration(durationMs: number): string {
  const durationSeconds = durationMs / 1000;

  if (durationSeconds < 60) {
    return `${new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(durationSeconds)}s`;
  }

  const durationMinutes = durationSeconds / 60;
  if (durationMinutes < 60) {
    return `${new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(durationMinutes)}m`;
  }

  const durationHours = durationMinutes / 60;
  return `${new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(durationHours)}h`;
}

type SchemaTypeNode =
  | { kind: 'primitive'; value: string }
  | { kind: 'array'; element: SchemaTypeNode }
  | { kind: 'map'; key: SchemaTypeNode; value: SchemaTypeNode }
  | { kind: 'struct'; fields: Array<{ name: string; type: SchemaTypeNode }> };

function splitTopLevel(value: string, delimiter: string): string[] {
  const parts: string[] = [];
  let start = 0;
  let angleDepth = 0;
  let parenDepth = 0;

  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];

    if (char === '<') {
      angleDepth += 1;
      continue;
    }

    if (char === '>') {
      angleDepth -= 1;
      continue;
    }

    if (char === '(') {
      parenDepth += 1;
      continue;
    }

    if (char === ')') {
      parenDepth -= 1;
      continue;
    }

    if (char === delimiter && angleDepth === 0 && parenDepth === 0) {
      parts.push(value.slice(start, index).trim());
      start = index + 1;
    }
  }

  parts.push(value.slice(start).trim());

  return parts.filter((part) => part.length > 0);
}

function findTopLevelColon(value: string): number {
  let angleDepth = 0;
  let parenDepth = 0;

  for (let index = 0; index < value.length; index += 1) {
    const char = value[index];

    if (char === '<') {
      angleDepth += 1;
      continue;
    }

    if (char === '>') {
      angleDepth -= 1;
      continue;
    }

    if (char === '(') {
      parenDepth += 1;
      continue;
    }

    if (char === ')') {
      parenDepth -= 1;
      continue;
    }

    if (char === ':' && angleDepth === 0 && parenDepth === 0) {
      return index;
    }
  }

  return -1;
}

function unwrapGeneric(type: string, prefix: string): string | null {
  if (!type.startsWith(prefix) || !type.endsWith('>')) {
    return null;
  }

  return type.slice(prefix.length, -1).trim();
}

function parseSchemaType(type: string): SchemaTypeNode {
  const trimmedType = type.trim();

  const structInner = unwrapGeneric(trimmedType, 'struct<');
  if (structInner !== null) {
    if (structInner.length === 0) {
      return { kind: 'struct', fields: [] };
    }

    const fieldEntries = splitTopLevel(structInner, ',');
    const fields = fieldEntries.map((entry) => {
      const colonIndex = findTopLevelColon(entry);

      if (colonIndex === -1) {
        return null;
      }

      return {
        name: entry.slice(0, colonIndex).trim(),
        type: parseSchemaType(entry.slice(colonIndex + 1)),
      };
    });

    if (fields.some((field) => field === null)) {
      return { kind: 'primitive', value: trimmedType };
    }

    return {
      kind: 'struct',
      fields: fields as Array<{ name: string; type: SchemaTypeNode }>,
    };
  }

  const arrayInner = unwrapGeneric(trimmedType, 'array<');
  if (arrayInner !== null) {
    return {
      kind: 'array',
      element: parseSchemaType(arrayInner),
    };
  }

  const mapInner = unwrapGeneric(trimmedType, 'map<');
  if (mapInner !== null) {
    const parts = splitTopLevel(mapInner, ',');

    if (parts.length !== 2) {
      return { kind: 'primitive', value: trimmedType };
    }

    return {
      kind: 'map',
      key: parseSchemaType(parts[0]),
      value: parseSchemaType(parts[1]),
    };
  }

  return { kind: 'primitive', value: trimmedType };
}

function indentLines(lines: string[]): string[] {
  return lines.map((line) => `  ${line}`);
}

function renderSchemaType(node: SchemaTypeNode): string[] {
  switch (node.kind) {
    case 'primitive':
      return [node.value];
    case 'array': {
      const elementLines = renderSchemaType(node.element);

      if (elementLines.length === 1) {
        return [`array<${elementLines[0]}>`];
      }

      return ['array<', ...indentLines(elementLines), '>'];
    }
    case 'map': {
      const keyLines = renderSchemaType(node.key);
      const valueLines = renderSchemaType(node.value);

      if (keyLines.length === 1 && valueLines.length === 1) {
        return [`map<${keyLines[0]}, ${valueLines[0]}>`];
      }

      return [
        'map<',
        ...indentLines([`key: ${keyLines[0]}`, ...indentLines(keyLines.slice(1))]),
        ...indentLines([`value: ${valueLines[0]}`, ...indentLines(valueLines.slice(1))]),
        '>',
      ];
    }
    case 'struct': {
      if (node.fields.length === 0) {
        return ['struct {}'];
      }

      return [
        'struct {',
        ...node.fields.flatMap((field) => {
          const typeLines = renderSchemaType(field.type);

          return [
            `  ${field.name}: ${typeLines[0]}`,
            ...indentLines(typeLines.slice(1)),
          ];
        }),
        '}',
      ];
    }
  }
}

export function formatSchemaType(type: string): string {
  return renderSchemaType(parseSchemaType(type)).join('\n');
}
