import {objectValue, orderedFields, prettyJson, resolveSchema, schemaValue, valueSchema} from './artifactSchema';
import type {SchemaNode, SchemaObject} from './artifactSchema';
import type {JsonValue} from './models';

export type ValuePath = readonly (string | number)[];

export type ValueEntry = {
  readonly path: ValuePath;
  readonly name: string;
  readonly value: JsonValue;
  readonly schema: SchemaNode | undefined;
};

export function valueAtPath(value: JsonValue, path: ValuePath): JsonValue | undefined {
  let current: JsonValue | undefined = value;
  for (const key of path) {
    if (Array.isArray(current)) {
      if (typeof key !== 'number' || !Number.isInteger(key) || key < 0 || !Object.hasOwn(current, key)) {
        return undefined;
      }
      current = current[key];
    } else {
      const object = objectValue(current);
      if (!object || !Object.hasOwn(object, key)) {return undefined;}
      current = object[String(key)];
    }
  }
  return current;
}

export function entriesFor(
  value: JsonValue, schema: SchemaNode | undefined, root: SchemaObject, path: ValuePath,
): readonly ValueEntry[] {
  const resolved = valueSchema(schema, root, value);
  if (Array.isArray(value)) {
    const prefix = Array.isArray(resolved.prefixItems) ? resolved.prefixItems : [];
    return value.map((item, index) => ({
      path: [...path, index],
      name: `[${index}]`,
      value: item,
      schema: resolveSchema(schemaValue(index < prefix.length ? prefix[index] : resolved.items), root).schema,
    }));
  }
  const object = objectValue(value);
  if (!object) {return [];}
  const properties = objectValue(resolved.properties);
  return orderedFields(object, resolved).map((key) => ({
    path: [...path, key],
    name: key,
    value: object[key],
    schema: resolveSchema(schemaValue(properties && Object.hasOwn(properties, key) ?
      properties[key] : resolved.additionalProperties), root).schema,
  }));
}

export function valueType(value: JsonValue): string {
  return value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value;
}

export function valueSummary(value: JsonValue): string {
  if (Array.isArray(value)) {return `${value.length} ${value.length === 1 ? 'item' : 'items'}`;}
  const object = objectValue(value);
  if (object) {
    const count = Object.keys(object).length;
    return `${count} ${count === 1 ? 'field' : 'fields'}`;
  }
  const literal = typeof value === 'string' && value !== '' ? value : JSON.stringify(value);
  return typeof value === 'string' && literal.length > 200 ? `${literal.slice(0, 197)}...` : literal;
}

export function pointerFor(path: ValuePath): string {
  return path.map((part) => `/${String(part).replaceAll('~', '~0').replaceAll('/', '~1')}`).join('');
}

export function copyValue(value: JsonValue, asJson = false): string {
  return typeof value === 'string' && !asJson ? value : prettyJson(value);
}

export function entryLabel(name: string, value: JsonValue): string {
  const object = objectValue(value);
  if (object) {
    for (const key of ['name', 'title', 'id']) {
      const identity = Object.hasOwn(object, key) ? object[key] : undefined;
      if ((typeof identity === 'string' && identity.trim() !== '') || typeof identity === 'number') {
        const text = String(identity);
        return `${name} (${text.length > 80 ? `${text.slice(0, 77)}...` : text})`;
      }
    }
  }
  return name;
}

export function findValues(value: JsonValue, query: string, limit = 100): {
  readonly matches: readonly {readonly path: ValuePath; readonly value: JsonValue}[];
  readonly truncated: boolean;
} {
  const matches: {readonly path: ValuePath; readonly value: JsonValue}[] = [];
  const needle = query.toLowerCase();
  if (needle === '') {return {matches, truncated: false};}
  const path: (string | number)[] = [];
  const stack: {
    readonly value: JsonValue[] | SchemaObject;
    readonly keys: Iterator<string | number>;
  }[] = [];
  let current = value;
  let name = '';
  while (true) {
    const scalar = current === null || typeof current !== 'object' ? String(current) : '';
    if (name.toLowerCase().includes(needle) || scalar.toLowerCase().includes(needle)) {
      if (matches.length >= limit) {return {matches, truncated: true};}
      matches.push({path: [...path], value: current});
    }
    if (Array.isArray(current)) {
      stack.push({value: current, keys: current.keys()});
    } else {
      const object = objectValue(current);
      if (object) {stack.push({value: object, keys: Object.keys(object).values()});}
    }
    let next: JsonValue | undefined;
    while (stack.length > 0) {
      const frame = stack[stack.length - 1];
      const key = frame.keys.next();
      if (key.done) {
        stack.pop();
        continue;
      }
      path.length = stack.length - 1;
      path.push(key.value);
      name = typeof key.value === 'string' ? key.value : '';
      next = Array.isArray(frame.value) ? frame.value[Number(key.value)] : frame.value[String(key.value)];
      break;
    }
    if (next === undefined) {return {matches, truncated: false};}
    current = next;
  }
}
