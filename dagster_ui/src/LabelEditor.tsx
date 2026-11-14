import {Button, TextArea} from '@dagster-io/ui-components';
import type {Draft} from './decisions';
import {changeEdit, changeEditValue} from './decisions';
import type {JsonValue} from './models';
import {parseEdit, reviewControls} from './labelCorrections';
import {cellText} from './api';
import styles from './ui.module.css';

export function LabelEditor({values, columns, draft, disabled, onDraft}: {
  readonly values: Readonly<Record<string, JsonValue>>;
  readonly columns: readonly string[];
  readonly draft: Draft; readonly disabled: boolean;
  readonly onDraft: (draft: Draft) => void;
}) {
  return <div className={styles.stack} role="group" aria-label="Configured review values">
    <strong>Configured review values</strong>
    <div>Edits apply to every occurrence of this review identity and are saved only with Approve.
      Original values and immutable evidence remain visible. The server validates permitted values.</div>
    {reviewControls(values, columns).map(({field, original}) => {
      const edited = Object.prototype.hasOwnProperty.call(draft.edits, field);
      const value = edited ? draft.edits[field] ?? null : original;
      const invalid = draft.invalidEdits[field];
      const input = invalid ?? (typeof value === 'string' && typeof original === 'string'
        ? value : JSON.stringify(value, null, 2));
      const error = invalid === undefined ? undefined : parseEdit(original, invalid).error;
      return <div key={field} className={styles.stack}>
        <div>Original {field}: <pre className={styles.literal}>{cellText(original)}</pre></div>
        <label className={styles.field}>Human {field}
          {typeof original === 'boolean' ? <select className={styles.select}
            aria-label={`Human ${field}`} value={String(value)} disabled={disabled}
            onChange={(event) => onDraft(changeEdit(draft, field, original, event.target.value))}>
            <option value="true">true</option><option value="false">false</option><option value="null">null</option>
          </select> : <TextArea aria-label={`Human ${field}`} value={input}
            aria-invalid={error !== undefined} disabled={disabled || (value === null && original !== null && edited)}
            onChange={(event) => onDraft(changeEdit(draft, field, original, event.target.value))} />}
        </label>
        {error && <div role="alert">{field}: {error}</div>}
        <div>{edited ? 'Staged human edit' : 'Using original value'}</div>
        <div className={styles.toolbar}>
          <Button disabled={disabled || (!edited && invalid === undefined)}
            onClick={() => onDraft(changeEditValue(draft, field, undefined))}>Use original {field}</Button>
          <Button disabled={disabled || value === null}
            onClick={() => onDraft(changeEditValue(draft, field, null))}>Set {field} to null</Button>
        </div>
      </div>;
    })}
  </div>;
}
