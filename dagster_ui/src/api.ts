import {z} from 'zod';

export class ApiError extends Error {
  constructor(readonly status: number, readonly code: string, message: string) {
    super(message);
    this.name = 'ApiError';
  }
}

const errorSchema = z.object({error: z.object({code: z.string(), message: z.string()})});

export async function request<T>(
  path: string,
  schema: z.ZodType<T>,
  method: 'GET' | 'POST' | 'PUT',
  body: unknown,
  signal?: AbortSignal,
): Promise<T> {
  const timeout = AbortSignal.timeout(30000);
  const response = await fetch(`/codebench/api${path}`, {
    method,
    credentials: 'same-origin',
    headers: body === undefined ? {Accept: 'application/json'} :
      {Accept: 'application/json', 'Content-Type': 'application/json'},
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
    cache: 'no-store',
  });
  const value: unknown = await response.json();
  if (!response.ok) {
    const parsed = errorSchema.safeParse(value);
    throw new ApiError(response.status, parsed.success ? parsed.data.error.code : 'http_error',
      parsed.success ? parsed.data.error.message : `HTTP ${response.status}`);
  }
  const parsed = schema.safeParse(value);
  if (!parsed.success) {
    throw new ApiError(502, 'invalid_response', 'The API returned an invalid response.');
  }
  return parsed.data;
}

export function failure(error: unknown): string {
  return error instanceof Error ? error.message : 'Request failed.';
}

export function cellText(value: unknown): string {
  return typeof value === 'string' ? value : jsonText(value);
}

function jsonText(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map((entry: unknown) => jsonText(entry)).join(',')}]`;
  }
  if (value !== null && typeof value === 'object') {
    return `{${Object.entries(value).sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
      .map(([key, entry]) => `${JSON.stringify(key)}:${jsonText(entry)}`).join(',')}}`;
  }
  return JSON.stringify(value) ?? '';
}

export function panelLink(search: string, view: string): string {
  const query = new URLSearchParams(search);
  query.set('view', view);
  return `?${query.toString()}`;
}
