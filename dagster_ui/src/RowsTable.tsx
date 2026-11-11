import {Button, Table} from '@dagster-io/ui-components';
import type {Page, Query, Row} from './models';
import {cellText} from './api';
import {toggleSort} from './query';
import styles from './ui.module.css';

export function RowsTable<T extends Row>({page, query, onChange, onSelect, disabled = false}: {
  readonly page: Omit<Page, 'rows'> & {readonly rows: readonly T[]};
  readonly query: Query; readonly onChange: (query: Query) => void;
  readonly onSelect: (row: T) => void; readonly disabled?: boolean;
}) {
  return <div className={styles.stack}>
    <div role="status">{page.matched_rows} filtered / {page.total_rows} total rows</div>
    <div className={styles.reel} tabIndex={0} role="region" aria-label="Full paginated table">
      <Table><thead><tr><th scope="col">Inspect</th>{page.columns.map((column) => {
        const sort = query.sort.find((entry) => entry.field === column);
        return <th key={column} scope="col" aria-sort={sort ? sort.direction === 'asc' ? 'ascending' : 'descending' : 'none'}>
          <Button disabled={disabled} onClick={() => onChange(toggleSort(query, column))}>
            {column}{sort ? ` (${sort.direction})` : ''}
          </Button>
        </th>;
      })}</tr></thead>
        <tbody>{page.rows.map((row) => <tr key={row.row_index}>
          <td><Button disabled={disabled} onClick={() => onSelect(row)}>Row {row.row_index + 1}</Button></td>
          {page.columns.map((column) => <td key={column}><div className={styles.preview}
            tabIndex={0} role="region" aria-label={`${column}, row ${row.row_index + 1}`}>
            {cellText(row.values[column])}
          </div></td>)}
        </tr>)}</tbody>
      </Table>
    </div>
    {page.rows.length === 0 && <div role="status">{page.total_rows === 0 ? 'This persisted table is empty.' : 'No rows match this query. Reset query to see all rows.'}</div>}
    <div className={styles.toolbar}>
      <Button disabled={disabled || page.offset === 0} onClick={() => onChange({...query, offset: Math.max(0, page.offset - page.limit)})}>Previous</Button>
      <span>{page.rows.length ? page.offset + 1 : 0}-{page.offset + page.rows.length} of {page.matched_rows}</span>
      <Button disabled={disabled || page.offset + page.limit >= page.matched_rows}
        onClick={() => onChange({...query, offset: page.offset + page.limit})}>Next</Button>
    </div>
  </div>;
}
