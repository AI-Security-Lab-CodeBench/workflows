import type {JsonValue} from './models';

export type SchemaObject = Readonly<Record<string, JsonValue>>;
export type SchemaNode = SchemaObject | boolean;

export function objectValue(value: JsonValue | undefined): SchemaObject | undefined {
  return value !== null && typeof value === 'object' && !Array.isArray(value) ? value : undefined;
}

export function schemaValue(value: JsonValue | undefined): SchemaNode | undefined {
  return typeof value === 'boolean' ? value : objectValue(value);
}

export function resolveSchema(node: SchemaNode | undefined, root: SchemaObject, ancestors: readonly string[] = []): {
  readonly schema: SchemaNode | undefined;
  readonly references: readonly string[];
  readonly unresolved?: string;
} {
  if (typeof node !== 'object' || typeof node.$ref !== 'string') {
    return {schema: node, references: ancestors};
  }
  const ref = node.$ref;
  if (ancestors.includes(ref)) {return {schema: node, references: ancestors, unresolved: ref};}
  let target: JsonValue | undefined = root;
  if (ref !== '#' && !ref.startsWith('#/')) {
    return {schema: node, references: ancestors, unresolved: ref};
  }
  let pointer: string;
  try {
    pointer = decodeURIComponent(ref.slice(1));
  } catch (error) {
    if (!(error instanceof URIError)) {throw error;}
    return {schema: node, references: ancestors, unresolved: ref};
  }
  for (const part of pointer === '' ? [] : pointer.slice(1).split('/')) {
    const key = part.replaceAll('~1', '/').replaceAll('~0', '~');
    if (Array.isArray(target)) {
      target = /^(0|[1-9]\d*)$/.test(key) ? target[Number(key)] : undefined;
    } else {
      const object = objectValue(target);
      target = object && Object.hasOwn(object, key) ? object[key] : undefined;
    }
  }
  const referenced = schemaValue(target);
  if (referenced === undefined) {return {schema: node, references: ancestors, unresolved: ref};}
  const resolved = resolveSchema(referenced, root, [...ancestors, ref]);
  if (typeof resolved.schema !== 'object') {return resolved;}
  const {$ref, ...siblings} = node;
  return {...resolved, schema: {...resolved.schema, ...siblings}};
}

export function schemaType(node: SchemaNode | undefined, root: SchemaObject, ancestors: readonly string[] = []): string {
  const resolved = resolveSchema(node, root, ancestors);
  const schema = resolved.schema;
  if (resolved.unresolved) {return `reference ${resolved.unresolved}`;}
  if (schema === true || schema === undefined) {return 'any';}
  if (schema === false) {return 'never';}
  for (const keyword of ['anyOf', 'oneOf', 'allOf'] as const) {
    const branches = schema[keyword];
    if (Array.isArray(branches)) {
      const types = branches.map((branch) => schemaType(schemaValue(branch), root, resolved.references));
      return [...new Set(types)].join(keyword === 'allOf' ? ' & ' : ' | ');
    }
  }
  if (Array.isArray(schema.type)) {return schema.type.filter((type) => typeof type === 'string').join(' | ');}
  if (typeof schema.type === 'string') {return schema.type;}
  if (schema.const === null) {return 'null';}
  if (Object.hasOwn(schema, 'const')) {return typeof schema.const;}
  if (schema.properties !== undefined || schema.additionalProperties !== undefined) {return 'object';}
  if (schema.items !== undefined || schema.prefixItems !== undefined) {return 'array';}
  return 'any';
}

export function valueSchema(
  node: SchemaNode | undefined, root: SchemaObject, value: JsonValue, ancestors: readonly string[] = [],
): SchemaObject {
  const resolved = resolveSchema(node, root, ancestors);
  if (typeof resolved.schema !== 'object' || resolved.unresolved) {return {};}
  const schema = resolved.schema;
  const branches = schema.anyOf ?? schema.oneOf;
  if (Array.isArray(branches)) {
    const kind = value === null ? 'null' : Array.isArray(value) ? 'array' : typeof value;
    const branch = branches.find((entry) => {
      const type = schemaType(schemaValue(entry), root, resolved.references);
      return type.split(' | ').includes(kind) || (kind === 'number' && type === 'integer');
    });
    if (branch !== undefined) {
      return {...schema, ...valueSchema(schemaValue(branch), root, value, resolved.references)};
    }
  }
  return schema;
}

export function orderedFields(value: SchemaObject, schema: SchemaObject): readonly string[] {
  const declared = Object.keys(objectValue(schema.properties) ?? {});
  return [...declared.filter((key) => Object.hasOwn(value, key)),
    ...Object.keys(value).filter((key) => !declared.includes(key))];
}

export function prettyJson(value: JsonValue): string {
  return JSON.stringify(value, null, 2);
}
