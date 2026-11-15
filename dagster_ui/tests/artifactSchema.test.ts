import assert from 'node:assert/strict';
import {test} from 'node:test';
import {artifactResponse} from '../src/artifacts';
import {orderedFields, prettyJson, resolveSchema, schemaType, valueSchema} from '../src/artifactSchema';
import type {SchemaObject} from '../src/artifactSchema';

test('artifact boundary preserves schema source, definitions and property order for both variants', () => {
  const schema = {
    type: 'object',
    properties: {category: {type: 'string'}, prompt: {$ref: '#/$defs/Prompt'}},
    $defs: {Prompt: {type: 'string', minLength: 1}},
    required: ['category'],
  };
  for (const source of ['declared', 'storage']) {
    for (const artifact of [
      {kind: 'json', value: {category: 'example', prompt: 'text'}},
      {kind: 'table', publication_state: null,
        page: {columns: [], total_rows: 0, matched_rows: 0, offset: 0, limit: 25, rows: []}},
    ]) {
      const data = artifactResponse(['qa', 'artifact'], null).parse({
        ...artifact, asset_key: ['qa', 'artifact'], partition_key: null, snapshot_id: 'a'.repeat(64),
        data_schema: {source, schema},
      });
      assert.deepEqual(data.data_schema, {source, schema});
      assert.deepEqual(Object.keys(data.data_schema.schema.properties ?? {}), ['category', 'prompt']);
    }
  }
});

test('artifact boundary rejects missing or invented schema provenance and nonobject root schemas', () => {
  const artifact = {
    kind: 'json', value: null, asset_key: ['qa'], partition_key: null, snapshot_id: 'a'.repeat(64),
  };
  for (const data_schema of [undefined, {source: 'inferred', schema: {}},
    {source: 'storage', schema: null}, {source: 'declared', schema: []}]) {
    assert.equal(artifactResponse(['qa'], null).safeParse({...artifact, data_schema}).success, false);
  }
});

test('data field order follows declared properties and retains undeclared and empty values', () => {
  const value = {prompt: 'long\ntext', extra: null, category: '', enabled: false};
  assert.deepEqual(orderedFields(value, {properties: {category: {}, enabled: {}, prompt: {}, absent: {}}}),
    ['category', 'enabled', 'prompt', 'extra']);
  assert.deepEqual(orderedFields(value, {}), ['prompt', 'extra', 'category', 'enabled']);
});

test('local references resolve escaped keys, nested pointers, array indices and sibling constraints', () => {
  const root = {
    $defs: {'a/b~c': {anyOf: [{type: 'string'}, {type: 'null'}]}},
  };
  assert.deepEqual(resolveSchema({$ref: '#/$defs/a~1b~0c/anyOf/0', minLength: 3}, root).schema,
    {type: 'string', minLength: 3});
  assert.equal(schemaType({$ref: '#/$defs/a~1b~0c'}, root), 'string | null');
});

test('schema references stop at cycles, missing targets and external locations without fetching', () => {
  const root = {$defs: {A: {$ref: '#/$defs/B'}, B: {$ref: '#/$defs/A'}}};
  for (const ref of ['#/$defs/A', '#/$defs/missing', 'https://example.invalid/schema', '#/%not-encoded']) {
    assert.ok(resolveSchema({$ref: ref}, root).unresolved);
    assert.match(schemaType({$ref: ref}, root), /^reference /);
  }
  assert.equal(resolveSchema({$ref: '#/toString'}, {}).unresolved, '#/toString');
  assert.equal(resolveSchema({$ref: '#/$defs/closed'}, {$defs: {closed: false}}).schema, false);
});

test('nullable referenced objects preserve field order for structured data and array items', () => {
  const root: SchemaObject = {$defs: {Item: {
    type: 'object', properties: {id: {type: 'string'}, detail: {type: 'string'}},
  }}};
  const schema: SchemaObject = {anyOf: [{type: 'null'}, {$ref: '#/$defs/Item'}]};
  assert.deepEqual(orderedFields({detail: 'text', id: 'one'}, valueSchema(schema, root, {detail: 'text', id: 'one'})),
    ['id', 'detail']);
  assert.equal(schemaType({type: ['array', 'null']}, root), 'array | null');
  assert.equal(schemaType(false, root), 'never');
});

test('raw JSON preserves full multiline and HTML-like content, nested empties and scalar values', () => {
  const value = {
    prompt: `  <script>inert</script>\n${'full content '.repeat(1000)}\n  last line`,
    values: [null, false, 0, '', [], {}],
  };
  assert.deepEqual(JSON.parse(prettyJson(value)), value);
  assert.ok(prettyJson(value).includes('\n  "prompt"'));
});
