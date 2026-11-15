import {useMemo, useRef, useState} from 'react';
import type {JsonValue} from './models';
import type {SchemaNode, SchemaObject} from './artifactSchema';
import {valueSchema} from './artifactSchema';
import {FullscreenInspector} from './FullscreenInspector';
import {copyValue, entriesFor, entryLabel, findValues, pointerFor, valueAtPath, valueSummary, valueType} from './structuredValue';
import type {ValueEntry, ValuePath} from './structuredValue';
import styles from './ui.module.css';

const pageSize = 50;

export function StructuredInspector({value, schema}: {readonly value: JsonValue; readonly schema: SchemaObject}) {
  const [path, setPath] = useState<ValuePath>([]);
  const [query, setQuery] = useState('');
  const [outline, setOutline] = useState(false);
  const [modal, setModal] = useState<{readonly entry: ValueEntry; readonly open: boolean}>();
  const [notice, setNotice] = useState('');
  const heading = useRef<HTMLHeadingElement>(null);
  const selectedValue = valueAtPath(value, path);
  const selected = selectedValue === undefined ? value : selectedValue;
  const selectedPath = selectedValue === undefined ? [] : path;
  const selectedSchema = useMemo(() => {
    let node: SchemaNode | undefined = schema;
    let current = value;
    for (let index = 0; index < selectedPath.length; index++) {
      const child: ValueEntry | undefined = entriesFor(current, node, schema, selectedPath.slice(0, index))
        .find((entry) => entry.path[index] === selectedPath[index]);
      if (!child) {return undefined;}
      current = child.value;
      node = child.schema;
    }
    return node;
  }, [schema, selectedPath, value]);
  const matches = useMemo(() => findValues(value, query), [value, query]);
  const rootEntry: ValueEntry = {name: 'Root', path: [], value, schema};
  const currentEntry: ValueEntry = {
    name: selectedPath.length ? (typeof selectedPath[selectedPath.length - 1] === 'number'
      ? `[${selectedPath[selectedPath.length - 1]}]` : String(selectedPath[selectedPath.length - 1])) : 'Root',
    path: selectedPath, value: selected, schema: selectedSchema,
  };

  function select(next: ValuePath) {
    setPath(next);
    setOutline(false);
    setNotice('');
    heading.current?.focus();
  }

  function openValue(entry: ValueEntry) {
    setModal({entry, open: true});
  }

  async function copy(text: string, label: string) {
    setNotice('');
    if (!navigator.clipboard) {
      setNotice('Clipboard unavailable. Open the full value to select and copy its text.');
      return;
    }
    try {
      await navigator.clipboard.writeText(text);
      setNotice(`Copied ${label}.`);
    } catch (error) {
      if (!(error instanceof Error)) {throw error;}
      setNotice('Copy failed. Open the full value to select and copy its text.');
    }
  }

  return <div className={styles.structuredInspector}>
    <div className={styles.inspectorSearch}>
      <label className={styles.field}>Find a property or value
        <input className={styles.select} type="search" value={query}
          placeholder="Search all nested values" onChange={(event) => setQuery(event.target.value)} />
      </label>
      {query && <button className={styles.valueButton} onClick={() => setQuery('')}>Clear search</button>}
      <button className={`${styles.valueButton} ${styles.outlineToggle}`} aria-expanded={outline}
        onClick={() => setOutline(!outline)}>Outline</button>
    </div>
    {query.trim() && <section className={styles.searchResults} aria-label="Search results">
      <div role="status" className={styles.schemaMeta}>
        {matches.matches.length}{matches.truncated ? '+' : ''} matches in loaded data
      </div>
      {matches.matches.map((match) => <button key={pointerFor(match.path)} className={styles.searchResult}
        onClick={() => select(match.path)}>
        <code>{pointerFor(match.path) || 'Root'}</code><span>{valueSummary(match.value)}</span>
      </button>)}
      {matches.truncated && <div className={styles.schemaMeta}>Showing the first 100 matches. Refine your search.</div>}
    </section>}
    <div className={styles.inspectorLayout}>
      <nav className={`${styles.valueOutline} ${outline ? styles.outlineOpen : ''}`} aria-label="Value outline">
        <div className={styles.outlineTitle}>Properties</div>
        <OutlineEntry entry={rootEntry} root={schema} selected={selectedPath} onSelect={select} initialOpen />
      </nav>
      <section className={styles.valueDetail} aria-label="Selected value">
        <nav className={styles.valueBreadcrumbs} aria-label="Value path">
          <button className={styles.valueButton} onClick={() => select([])}>Root</button>
          {selectedPath.map((part, index) => <span key={pointerFor(selectedPath.slice(0, index + 1))}>
            <span aria-hidden="true">/</span>
            <button className={styles.valueButton} onClick={() => select(selectedPath.slice(0, index + 1))}>
              {typeof part === 'number' ? `[${part}]` : part || '""'}
            </button>
          </span>)}
        </nav>
        <div className={styles.detailHeader}>
          <div className={styles.artifactLabel}>
            <h3 ref={heading} tabIndex={-1}>{currentEntry.name || '""'}</h3>
            <span className={styles.valueType}>{valueType(selected)}</span>
          </div>
          <ValueActions entry={currentEntry} onCopy={copy} onOpen={openValue} />
        </div>
        <div role="status" className={styles.copyNotice}>{notice}</div>
        <PropertyList key={pointerFor(selectedPath)} entry={currentEntry} root={schema}
          onSelect={select} onCopy={copy} onOpen={openValue} />
      </section>
    </div>
    {modal && <ValueModal key={pointerFor(modal.entry.path)} entry={modal.entry} isOpen={modal.open}
      onClose={() => setModal({...modal, open: false})} />}
  </div>;
}

function OutlineEntry({entry, root, selected, onSelect, initialOpen = false}: {
  readonly entry: ValueEntry; readonly root: SchemaObject; readonly selected: ValuePath;
  readonly onSelect: (path: ValuePath) => void; readonly initialOpen?: boolean;
}) {
  const [expanded, setExpanded] = useState(initialOpen);
  const [limit, setLimit] = useState(pageSize);
  const container = entry.value !== null && typeof entry.value === 'object';
  const ancestor = entry.path.length < selected.length
    && entry.path.every((part, index) => part === selected[index]);
  const open = expanded || ancestor;
  const active = pointerFor(selected) === pointerFor(entry.path);
  const children = useMemo(() => open && container ? entriesFor(entry.value, entry.schema, root, entry.path) : [],
    [open, container, entry.value, entry.schema, root, entry.path]);
  const selectedIndex = children.findIndex((child) => child.path.every((part, index) => part === selected[index]));
  const start = selectedIndex >= limit ? Math.floor(selectedIndex / pageSize) * pageSize : 0;
  return <div className={styles.outlineBranch}>
    <div className={styles.outlineRow}>
      {container ? <button className={styles.outlineDisclosure} aria-label={`${open ? 'Collapse' : 'Expand'} ${entry.name}`}
        aria-expanded={open} onClick={() => {
          if (ancestor) {onSelect(entry.path);}
          setExpanded(!open);
        }}>{open ? '-' : '+'}</button> : <span className={styles.outlineSpacer} />}
      <button className={styles.outlineItem} aria-current={active ? 'true' : undefined}
        title={entryLabel(entry.name, entry.value)} onClick={() => onSelect(entry.path)}>
        {entryLabel(entry.name, entry.value)}
        {container && <span>{Array.isArray(entry.value) ? entry.value.length : Object.keys(entry.value).length}</span>}
      </button>
    </div>
    {open && container && <div className={styles.outlineChildren}>
      {start > 0 && <button className={styles.valueButton} onClick={() => {
        onSelect(entry.path); setLimit(pageSize);
      }}>Back to first items</button>}
      {children.slice(start, start + limit).map((child) =>
        <OutlineEntry key={pointerFor(child.path)} entry={child} root={root} selected={selected} onSelect={onSelect} />)}
      {children.length > start + limit && <button className={styles.valueButton}
        onClick={() => setLimit(limit + pageSize)}>Show next {Math.min(pageSize, children.length - start - limit)}</button>}
    </div>}
  </div>;
}

function ValueActions({entry, onCopy, onOpen, compact = false}: {
  readonly entry: ValueEntry; readonly onCopy: (text: string, label: string) => Promise<void>;
  readonly onOpen: (entry: ValueEntry) => void; readonly compact?: boolean;
}) {
  return <div className={styles.valueActions}>
    <button className={styles.valueButton} aria-label={`Copy value ${pointerFor(entry.path) || 'Root'}`}
      onClick={() => void onCopy(copyValue(entry.value), entry.name)}>Copy</button>
    <button className={styles.valueButton} aria-label={`Open full value ${pointerFor(entry.path) || 'Root'}`}
      title="Open full value" onClick={() => onOpen(entry)}>{compact ? 'View' : 'Open full value'}</button>
  </div>;
}

function PropertyList({entry, root, onSelect, onCopy, onOpen}: {
  readonly entry: ValueEntry; readonly root: SchemaObject; readonly onSelect: (path: ValuePath) => void;
  readonly onCopy: (text: string, label: string) => Promise<void>; readonly onOpen: (entry: ValueEntry) => void;
}) {
  const [page, setPage] = useState(0);
  const children = useMemo(() => entriesFor(entry.value, entry.schema, root, entry.path), [entry, root]);
  const description = valueSchema(entry.schema, root, entry.value).description;
  return <>
    {typeof description === 'string' && <p className={styles.schemaDescription}>{description}</p>}
    {children.length ? <>
      <dl className={styles.propertyList}>
        {children.slice(page * pageSize, (page + 1) * pageSize).map((child) => {
          const childDescription = valueSchema(child.schema, root, child.value).description;
          const container = child.value !== null && typeof child.value === 'object';
          return <div key={pointerFor(child.path)} className={styles.propertyRow}>
            <dt>
              <button className={styles.propertyName} onClick={() => onSelect(child.path)}>
                {entryLabel(child.name, child.value)}
              </button>
              <span className={styles.valueType}>{valueType(child.value)}</span>
            </dt>
            <dd>
              {typeof childDescription === 'string' && <div className={styles.propertyDescription}>{childDescription}</div>}
              {container ? <button className={styles.propertyNavigate} onClick={() => onSelect(child.path)}>
                {valueSummary(child.value)} <span aria-hidden="true">&rarr;</span>
              </button> : <pre className={styles.propertyPreview}>{valueSummary(child.value)}</pre>}
              <ValueActions entry={child} onCopy={onCopy} onOpen={onOpen} compact />
            </dd>
          </div>;
        })}
      </dl>
      {children.length > pageSize && <div className={styles.valuePagination}>
        <button className={styles.valueButton} disabled={page === 0} onClick={() => setPage(page - 1)}>Previous</button>
        <span>{page * pageSize + 1}-{Math.min(children.length, (page + 1) * pageSize)} of {children.length}</span>
        <button className={styles.valueButton} disabled={(page + 1) * pageSize >= children.length}
          onClick={() => setPage(page + 1)}>Next</button>
      </div>}
    </> : <pre className={styles.selectedPreview}>{valueSummary(entry.value)}</pre>}
  </>;
}

function ValueModal({entry, isOpen, onClose}: {
  readonly entry: ValueEntry; readonly isOpen: boolean; readonly onClose: () => void;
}) {
  const [json, setJson] = useState(false);
  const [wrap, setWrap] = useState(true);
  const [find, setFind] = useState('');
  const [match, setMatch] = useState(0);
  const [notice, setNotice] = useState('');
  const text = useMemo(() => copyValue(entry.value, json), [entry.value, json]);
  const matches = useMemo(() => {
    const offsets: number[] = [];
    if (!find) {return offsets;}
    let index = text.indexOf(find);
    while (index !== -1) {
      offsets.push(index);
      index = text.indexOf(find, index + find.length);
    }
    return offsets;
  }, [text, find]);
  const currentMatch = matches.length ? Math.min(match, matches.length - 1) : 0;
  const offset = matches[currentMatch];

  async function copy(textToCopy: string, label: string) {
    setNotice('');
    if (!navigator.clipboard) {setNotice('Clipboard unavailable. Select the full text below to copy.'); return;}
    try {
      await navigator.clipboard.writeText(textToCopy);
      setNotice(`Copied ${label}.`);
    } catch (error) {
      if (!(error instanceof Error)) {throw error;}
      setNotice('Copy failed. Select the full text below to copy.');
    }
  }

  function download() {
    const url = URL.createObjectURL(new Blob([text], {type: json || typeof entry.value !== 'string'
      ? 'application/json;charset=utf-8' : 'text/plain;charset=utf-8'}));
    const link = document.createElement('a');
    link.href = url;
    link.download = `value.${json || typeof entry.value !== 'string' ? 'json' : 'txt'}`;
    link.click();
    URL.revokeObjectURL(url);
  }

  return <FullscreenInspector isOpen={isOpen} title={`Full value: ${entry.name || '""'}`} onClose={onClose}>
    <div className={styles.fullValue}>
      <div className={styles.fullValueTools}>
        <code className={styles.valuePath}>{pointerFor(entry.path) || 'Root (empty JSON Pointer)'}</code>
        <div className={styles.schemaMeta}>{valueType(entry.value)} · {text.length.toLocaleString()} characters · Complete loaded value</div>
        <div className={styles.toolbar}>
          <button className={styles.valueButton} onClick={() => void copy(copyValue(entry.value), 'value')}>Copy value</button>
          <button className={styles.valueButton} onClick={() => void copy(copyValue(entry.value, true), 'JSON')}>Copy as JSON</button>
          <button className={styles.valueButton} onClick={() => void copy(pointerFor(entry.path), 'path')}>Copy path</button>
          <button className={styles.valueButton} onClick={download}>Download</button>
          {typeof entry.value === 'string' && <label className={styles.inlineControl}>
            <input type="checkbox" checked={json} onChange={(event) => {setJson(event.target.checked); setMatch(0);}} />
            JSON representation
          </label>}
          <label className={styles.inlineControl}>
            <input type="checkbox" checked={wrap} onChange={(event) => setWrap(event.target.checked)} />Wrap lines
          </label>
        </div>
        <div className={styles.toolbar}>
          <label className={styles.field}>Find in value (case-sensitive)
            <input className={styles.select} type="search" value={find}
              onChange={(event) => {setFind(event.target.value); setMatch(0);}} />
          </label>
          <button className={styles.valueButton} disabled={!matches.length} onClick={() => {
            setMatch((currentMatch + 1) % matches.length);
          }}>Next match</button>
          {find && <span role="status">{matches.length ? currentMatch + 1 : 0} / {matches.length} matches</span>}
        </div>
        <div role="status" className={styles.copyNotice}>{notice}</div>
      </div>
      <pre tabIndex={0} aria-label="Complete value"
        className={`${styles.fullValueText} ${wrap ? styles.wrappedValue : ''}`}>
        {offset === undefined ? text : <>{text.slice(0, offset)}<mark key={`${find}:${currentMatch}`}
          ref={(element) => element?.scrollIntoView({block: 'nearest'})}>{text.slice(offset, offset + find.length)}</mark>
          {text.slice(offset + find.length)}</>}
      </pre>
    </div>
  </FullscreenInspector>;
}
