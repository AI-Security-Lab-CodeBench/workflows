import {useState} from 'react';
import type {ReactNode} from 'react';
import type {JsonValue} from './models';
import {objectValue, prettyJson, resolveSchema, schemaType, schemaValue} from './artifactSchema';
import type {SchemaNode as Schema, SchemaObject} from './artifactSchema';
import {StructuredInspector} from './StructuredInspector';
import styles from './ui.module.css';

export function ArtifactContent({value, dataSchema, table = false, children}: {
  readonly value: JsonValue;
  readonly dataSchema: {readonly source: 'declared' | 'storage'; readonly schema: SchemaObject};
  readonly table?: boolean;
  readonly children?: ReactNode;
}) {
  const [view, setView] = useState('data');
  return <div className={styles.stack}>
    <div className={styles.toolbar}>
      <label className={styles.field}>View
        <select className={styles.select} value={view} onChange={(event) => setView(event.target.value)}>
          <option value="data">Data</option><option value="schema">Schema</option>
          <option value="json">{table ? 'JSON (current page)' : 'JSON'}</option>
        </select>
      </label>
      <span className={styles.schemaMeta}>{dataSchema.source === 'declared' ? 'Declared schema' : 'Storage schema'}</span>
    </div>
    {view === 'data' && (table ? children : <StructuredInspector value={value} schema={dataSchema.schema} />)}
    {view === 'schema' && <div className={styles.stack}>
      <div className={styles.schemaMeta}>
        {table ? 'Schema describes one row. ' : 'Schema describes the artifact root. '}
        {dataSchema.source === 'declared'
          ? 'Declared by the artifact contract; this view does not validate stored values.'
          : 'Derived from storage structure, not a declared validation contract.'}
      </div>
      <SchemaNode node={dataSchema.schema} root={dataSchema.schema} name={table ? 'Row' : 'Root'} />
      <details className={styles.artifactGroup}><summary>Full schema JSON</summary>
        <pre className={styles.literal}>{prettyJson(dataSchema.schema)}</pre>
      </details>
    </div>}
    {view === 'json' && <div className={styles.stack}>
      {table && <div className={styles.schemaMeta}>Current page only; not the full dataset.</div>}
      <pre className={styles.literal}>{prettyJson(value)}</pre>
    </div>}
  </div>;
}

function Disclosure({label, children, accessibleLabel}: {
  readonly label: ReactNode; readonly children: ReactNode; readonly accessibleLabel?: string;
}) {
  const [open, setOpen] = useState(false);
  return <details className={styles.artifactGroup} onToggle={(event) => setOpen(event.currentTarget.open)}>
    <summary aria-label={accessibleLabel}>{label}</summary>
    {open && <div className={styles.artifactChildren}>{children}</div>}
  </details>;
}

function SchemaNode({node, root, name, required, ancestors = []}: {
  readonly node: Schema; readonly root: SchemaObject; readonly name: string;
  readonly required?: boolean; readonly ancestors?: readonly string[];
}) {
  const resolved = resolveSchema(node, root, ancestors);
  const schema = resolved.schema;
  if (typeof schema !== 'object') {
    return <div className={styles.artifactField}><strong>{name}</strong> <code>{schemaType(node, root)}</code>
      {required !== undefined && <span className={styles.schemaMeta}> {required ? 'required' : 'optional'}</span>}</div>;
  }
  const properties = objectValue(schema.properties);
  const requiredNames = Array.isArray(schema.required) ? schema.required : [];
  const hiddenKeywords = new Set(['properties', '$defs', 'definitions', 'items', 'prefixItems',
    'additionalProperties', 'anyOf', 'oneOf', 'allOf', 'description', 'title', 'type', 'required']);
  return <div className={styles.stack}>
    <div className={styles.artifactField}>
      <div className={styles.artifactLabel}><strong>{name}</strong><code>{schemaType(node, root, ancestors)}</code>
        {required !== undefined && <span className={styles.schemaMeta}>{required ? 'required' : 'optional'}</span>}
      </div>
      {typeof schema.title === 'string' && <div className={styles.schemaMeta}>{schema.title}</div>}
      {typeof schema.description === 'string' && <div className={styles.schemaDescription}>{schema.description}</div>}
      {resolved.unresolved && <div className={styles.schemaMeta}>Unexpanded reference: {resolved.unresolved}.
        Recursive or unavailable locally; inspect the full schema JSON.</div>}
      {Object.entries(schema).filter(([key]) => !hiddenKeywords.has(key)).map(([key, value]) =>
        <div key={key} className={styles.schemaConstraint}><code>{key}</code><pre className={styles.literal}>{prettyJson(value)}</pre></div>)}
    </div>
    {!resolved.unresolved && <>
      {properties && Object.entries(properties).map(([key, child]) => {
        const field = schemaValue(child);
        return field === undefined ? null : <Disclosure key={key} label={<><strong>{key}</strong>
          {' '}<code>{schemaType(field, root, resolved.references)}</code>
          {' '}<span className={styles.schemaMeta}>{requiredNames.includes(key) ? 'required' : 'optional'}</span></>}>
          <SchemaNode name={key} node={field} root={root} required={requiredNames.includes(key)} ancestors={resolved.references} />
        </Disclosure>;
      })}
      {['items', 'additionalProperties'].map((keyword) => {
        const child = schemaValue(schema[keyword]);
        return child === undefined ? null : <Disclosure key={keyword} label={<>{keyword} <code>{schemaType(child, root, resolved.references)}</code></>}>
          <SchemaNode name={keyword} node={child} root={root} ancestors={resolved.references} />
        </Disclosure>;
      })}
      {['anyOf', 'oneOf', 'allOf', 'prefixItems'].map((keyword) => {
        const branches = schema[keyword];
        return !Array.isArray(branches) ? null : <Disclosure key={keyword} label={`${keyword} (${branches.length})`}>
          {branches.map((child, index) => {
            const branch = schemaValue(child);
            return branch === undefined ? null : <SchemaNode key={index} name={`${keyword} [${index}]`}
              node={branch} root={root} ancestors={resolved.references} />;
          })}
        </Disclosure>;
      })}
      {['$defs', 'definitions'].map((keyword) => {
        const definitions = objectValue(schema[keyword]);
        return !definitions ? null : <Disclosure key={keyword} label={`${keyword} (${Object.keys(definitions).length})`}>
          {Object.entries(definitions).map(([key, child]) => {
            const definition = schemaValue(child);
            return definition === undefined ? null : <Disclosure key={key} label={key}>
              <SchemaNode name={key} node={definition} root={root} ancestors={resolved.references} />
            </Disclosure>;
          })}
        </Disclosure>;
      })}
    </>}
  </div>;
}
