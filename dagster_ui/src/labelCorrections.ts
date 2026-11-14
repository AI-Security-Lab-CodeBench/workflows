import {z} from 'zod';
import type {JsonValue} from './models';

export function reviewControls(values: Readonly<Record<string, JsonValue>>, columns: readonly string[]) {
  return columns.map((field) => ({field, original: values[field] ?? null}));
}

export function parseEdit(original: JsonValue, input: string):
  {readonly value: JsonValue; readonly error?: never} |
  {readonly error: string; readonly value?: never} {
  if (typeof original === 'string') {return {value: input};}
  let value: unknown;
  try {value = JSON.parse(input);}
  catch (error) {
    if (!(error instanceof SyntaxError)) {throw error;}
    return {error: 'Enter valid JSON.'};
  }
  const parsed = z.json().safeParse(value);
  if (!parsed.success) {return {error: 'Enter a JSON value.'};}
  const result = parsed.data;
  if (result !== null && original !== null &&
    (typeof result !== typeof original || Array.isArray(result) !== Array.isArray(original))) {
    return {error: `Keep the original ${Array.isArray(original) ? 'array' : typeof original} type or use null.`};
  }
  return {value: result};
}
