import {Button, TextInput} from '@dagster-io/ui-components';
import {useState} from 'react';
import type {Query} from './models';
import {initialQuery} from './models';
import styles from './ui.module.css';

export function QueryControls({query, columns, onChange, onRefresh, disabled = false}: {
  readonly query: Query; readonly columns: readonly string[];
  readonly onChange: (query: Query) => void; readonly onRefresh: () => void;
  readonly disabled?: boolean;
}) {
  const [field, setField] = useState('');
  const [operator, setOperator] = useState('contains');
  const [value, setValue] = useState('');
  const selectedField = columns.includes(field) ? field : columns[0] ?? '';
  function addFilter() {
    if (!selectedField || query.filters.length >= 20) {return;}
    switch (operator) {
      case 'is_null':
        onChange({...query, offset: 0, filters: [...query.filters, {field: selectedField, operator}]});
        break;
      case 'contains':
      case 'equals':
        onChange({...query, offset: 0, filters: [...query.filters, {field: selectedField, operator, value}]});
        break;
    }
  }
  return <div className={styles.stack}>
    <div className={styles.toolbar}>
      <label className={styles.field}>Search all columns
        <TextInput value={query.search} maxLength={2000} disabled={disabled}
          onChange={(e) => onChange({...query, search: e.target.value, offset: 0})} />
      </label>
      <label className={styles.field}>Page size
        <select className={styles.select} value={query.limit} disabled={disabled}
          onChange={(e) => onChange({...query, limit: Number(e.target.value), offset: 0})}>
          {[2, 10, 25, 50, 100, 200].map((size) => <option key={size}>{size}</option>)}
        </select>
      </label>
      <Button disabled={disabled} onClick={() => onChange({...initialQuery})}>Reset query</Button>
      <Button disabled={disabled} onClick={onRefresh}>Refresh snapshot</Button>
    </div>
    <div className={styles.toolbar}>
      <label className={styles.field}>Filter column
        <select className={styles.select} value={selectedField} disabled={disabled}
          onChange={(e) => setField(e.target.value)}>
          {columns.map((column) => <option key={column}>{column}</option>)}
        </select>
      </label>
      <label className={styles.field}>Operator
        <select className={styles.select} value={operator} disabled={disabled} onChange={(e) => setOperator(e.target.value)}>
          <option value="contains">Contains</option><option value="equals">Equals</option><option value="is_null">Is null</option>
        </select>
      </label>
      {operator !== 'is_null' && <label className={styles.field}>Filter value
        <TextInput value={value} maxLength={8192} disabled={disabled} onChange={(e) => setValue(e.target.value)} />
      </label>}
      <Button disabled={disabled || !selectedField || query.filters.length >= 20} onClick={addFilter}>Add filter</Button>
    </div>
    {query.filters.map((filter, index) => <div key={index} className={styles.toolbar}>
      <span>{filter.field} {filter.operator}{'value' in filter ? `: ${filter.value}` : ''}</span>
      <Button disabled={disabled} onClick={() => onChange({...query, offset: 0,
        filters: query.filters.filter((_, position) => position !== index)})}>Remove filter {index + 1}</Button>
    </div>)}
    {query.sort.length > 0 && <div className={styles.toolbar}>
      <span>Sort priority: {query.sort.map((sort) => `${sort.field} ${sort.direction}`).join(', ')}</span>
      <Button disabled={disabled} onClick={() => onChange({...query, sort: [], offset: 0})}>Clear sorting</Button>
    </div>}
  </div>;
}
