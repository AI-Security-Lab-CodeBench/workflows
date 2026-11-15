import assert from 'node:assert/strict';
import {test} from 'node:test';
import type {SchemaObject} from '../src/artifactSchema';
import type {JsonValue} from '../src/models';
import {
  copyValue, entriesFor, entryLabel, findValues, pointerFor, valueAtPath, valueSummary, valueType,
} from '../src/structuredValue';
import type {ValuePath} from '../src/structuredValue';

test('copy preserves exact strings by default and JSON literals for other values', () => {
  const cases: readonly (readonly [JsonValue, string, string])[] = [
    ['', '', '""'],
    [null, 'null', 'null'],
    [false, 'false', 'false'],
    [0, '0', '0'],
    ['  first\nlast\n', '  first\nlast\n', '"  first\\nlast\\n"'],
    ['x'.repeat(1000), 'x'.repeat(1000), `"${'x'.repeat(1000)}"`],
    [[], '[]', '[]'],
    [{empty: '', values: [null, false, 0]},
      '{\n  "empty": "",\n  "values": [\n    null,\n    false,\n    0\n  ]\n}',
      '{\n  "empty": "",\n  "values": [\n    null,\n    false,\n    0\n  ]\n}'],
  ];
  for (const [value, exact, json] of cases) {
    assert.equal(copyValue(value), exact);
    assert.equal(copyValue(value, true), json);
  }
});

test('paths retain verbatim keys and pointers escape each segment including empty keys', () => {
  const value = {'a/b~c': [{'': {'~1/': false}}]};
  const path: ValuePath = Object.freeze(['a/b~c', 0, '', '~1/']);
  assert.equal(valueAtPath(value, path), false);
  assert.equal(valueAtPath(value, []), value);
  assert.equal(pointerFor(path), '/a~1b~0c/0//~01~1');
  assert.equal(pointerFor([]), '');
  assert.equal(pointerFor(['']), '/');
  assert.deepEqual(path, ['a/b~c', 0, '', '~1/']);
});

test('path lookup rejects inherited properties, scalar traversal and nonnumeric array keys', () => {
  const object = {own: null, toString: 'own value'};
  Object.setPrototypeOf(object, {inherited: 'hidden'});
  const array: JsonValue[] = [false, 0];
  const cases: readonly (readonly [JsonValue, ValuePath, JsonValue | undefined])[] = [
    [object, ['own'], null],
    [object, ['toString'], 'own value'],
    [object, ['inherited'], undefined],
    [object, ['__proto__'], undefined],
    [{}, ['constructor'], undefined],
    [array, [0], false],
    [array, [1], 0],
    [array, [2], undefined],
    [array, [-1], undefined],
    [array, [0.5], undefined],
    [array, ['0'], undefined],
    [array, ['length'], undefined],
    ['text', ['length'], undefined],
    [null, ['child'], undefined],
    [false, ['child'], undefined],
  ];
  for (const [value, path, expected] of cases) {
    assert.equal(valueAtPath(value, path), expected);
  }
});

test('object entries use nullable referenced schemas and preserve undeclared fields and paths', () => {
  const root: SchemaObject = {$defs: {
    Item: {type: 'object', properties: {
      id: {$ref: '#/$defs/Id'}, 'a/b~c': false, missing: {type: 'number'},
    }, additionalProperties: {$ref: '#/$defs/Extra'}},
    Id: {type: 'string', minLength: 1},
    Extra: {type: 'boolean'},
  }};
  const value = Object.freeze({extra: false, 'a/b~c': null, id: ''});
  const path = Object.freeze(['rows', 2]);
  const schema: SchemaObject = {anyOf: [{type: 'null'}, {$ref: '#/$defs/Item'}]};
  assert.deepEqual(entriesFor(value, schema, root, path), [
    {path: ['rows', 2, 'id'], name: 'id', value: '', schema: {type: 'string', minLength: 1}},
    {path: ['rows', 2, 'a/b~c'], name: 'a/b~c', value: null, schema: false},
    {path: ['rows', 2, 'extra'], name: 'extra', value: false, schema: {type: 'boolean'}},
  ]);
  assert.deepEqual(path, ['rows', 2]);
  assert.deepEqual(value, {extra: false, 'a/b~c': null, id: ''});
});

test('array entries resolve prefixItems before items and keep numeric index paths', () => {
  const root = {$defs: {
    Tuple: {type: 'array', prefixItems: [{$ref: '#/$defs/First'}, false], items: {$ref: '#/$defs/Rest'}},
    First: {type: 'string'},
    Rest: {type: 'number'},
  }};
  assert.deepEqual(entriesFor(['', null, 0], {$ref: '#/$defs/Tuple'}, root, ['tuple']), [
    {path: ['tuple', 0], name: '[0]', value: '', schema: {type: 'string'}},
    {path: ['tuple', 1], name: '[1]', value: null, schema: false},
    {path: ['tuple', 2], name: '[2]', value: 0, schema: {type: 'number'}},
  ]);
  assert.deepEqual(entriesFor([false], undefined, {}, []), [
    {path: [0], name: '[0]', value: false, schema: undefined},
  ]);
});

test('schemaless entries retain object insertion order and scalars have no entries', () => {
  assert.deepEqual(entriesFor({'': '', other: []}, undefined, {}, []), [
    {path: [''], name: '', value: '', schema: undefined},
    {path: ['other'], name: 'other', value: [], schema: undefined},
  ]);
  for (const value of [null, false, 0, 'text', [], {}]) {
    assert.deepEqual(entriesFor(value, undefined, {}, []), []);
  }
});

test('value types and summaries distinguish empties, scalars and compact container counts', () => {
  const cases: readonly (readonly [JsonValue, string, string])[] = [
    [null, 'null', 'null'], [false, 'boolean', 'false'], [0, 'number', '0'],
    ['', 'string', '""'], ['a\nb', 'string', 'a\nb'],
    [[], 'array', '0 items'], [[null], 'array', '1 item'], [[0, false], 'array', '2 items'],
    [{}, 'object', '0 fields'], [{x: ''}, 'object', '1 field'],
    [{x: '', y: []}, 'object', '2 fields'],
  ];
  for (const [value, type, summary] of cases) {
    assert.equal(valueType(value), type);
    assert.equal(valueSummary(value), summary);
  }
  const long = `${'head'.repeat(100)}last`;
  const summary = valueSummary(long);
  assert.ok(summary.length <= 200);
  assert.ok(summary.startsWith('head'));
  assert.ok(!summary.includes('last'));
  assert.equal(copyValue(long), long);
});

test('entry labels use only existing bounded name, title or id identities', () => {
  const cases: readonly (readonly [JsonValue, string])[] = [
    [{name: 'Named', title: 'Titled', id: 4}, '[0] (Named)'],
    [{name: '', title: 'Titled'}, '[0] (Titled)'],
    [{name: null, title: {}, id: 0}, '[0] (0)'],
    [{name: ' '.repeat(2), id: 'record'}, '[0] (record)'],
    [{description: 'not an identity'}, '[0]'],
    [null, '[0]'], [[], '[0]'], ['text', '[0]'],
    [{name: 'n'.repeat(1000)}, `[0] (${'n'.repeat(77)}...)`],
  ];
  for (const [value, expected] of cases) {
    assert.equal(entryLabel('[0]', value), expected);
  }
  const inherited = {other: true};
  Object.setPrototypeOf(inherited, {name: 'inherited'});
  assert.equal(entryLabel('record', inherited), 'record');
});

test('search matches keys and full scalar values beneath every nested container', () => {
  const long = `${'x'.repeat(1000)}NeedLE\nend`;
  const value = Object.freeze({NeedleKey: Object.freeze({child: 'NEEDLE'}), nested: [{text: long}]});
  assert.deepEqual(findValues(value, 'needle'), {
    matches: [
      {path: ['NeedleKey'], value: {child: 'NEEDLE'}},
      {path: ['NeedleKey', 'child'], value: 'NEEDLE'},
      {path: ['nested', 0, 'text'], value: long},
    ],
    truncated: false,
  });
  assert.deepEqual(findValues({both: 'both'}, 'both').matches, [{path: ['both'], value: 'both'}]);
  for (const value of [null, false, 0]) {
    assert.deepEqual(findValues(value, String(value)), {matches: [{path: [], value}], truncated: false});
  }
  assert.deepEqual(findValues(value, ''), {matches: [], truncated: false});
  assert.deepEqual(findValues(value, 'absent'), {matches: [], truncated: false});
});

test('search traverses deeply nested values iteratively and retains the complete stable path', () => {
  let value: JsonValue = 'deep target';
  const path: (string | number)[] = [];
  for (let depth = 0; depth < 20000; depth++) {
    value = [value];
    path.push(0);
  }
  const result = findValues(value, 'target');
  assert.equal(result.truncated, false);
  assert.deepEqual(result.matches, [{path, value: 'deep target'}]);
});

test('search reports truncation only upon an extra match and stops before later values', () => {
  const value = {
    first: 'hit',
    second: 'HIT',
    get untouched(): JsonValue {throw new assert.AssertionError({message: 'search continued after the extra match'});},
  };
  assert.deepEqual(findValues(value, 'hit', 1), {
    matches: [{path: ['first'], value: 'hit'}], truncated: true,
  });
  assert.deepEqual(findValues(['hit', 'miss', 'HIT'], 'hit', 2), {
    matches: [{path: [0], value: 'hit'}, {path: [2], value: 'HIT'}], truncated: false,
  });
  assert.deepEqual(findValues(['hit'], 'hit', 0), {matches: [], truncated: true});
  assert.deepEqual(findValues(['miss'], 'hit', 0), {matches: [], truncated: false});
  const hundred = Array.from({length: 100}, () => 'hit');
  assert.equal(findValues(hundred, 'hit').matches.length, 100);
  assert.equal(findValues(hundred, 'hit').truncated, false);
  assert.deepEqual(findValues([...hundred, 'hit'], 'hit'), {
    matches: hundred.map((value, index) => ({path: [index], value})), truncated: true,
  });
});
