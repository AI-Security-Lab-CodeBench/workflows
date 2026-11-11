import {cellText} from './api';
import styles from './ui.module.css';

export function Fields({fields}: {readonly fields: Readonly<Record<string, unknown>>}) {
  return <dl className={`${styles.stack} ${styles.fields}`}>{Object.entries(fields).map(([name, value]) => <div key={name}>
    <dt><strong>{name}</strong></dt>
    <dd><FieldValue value={value} /></dd>
  </div>)}</dl>;
}

function FieldValue({value}: {readonly value: unknown}) {
  if (value !== null && typeof value === 'object' && Object.keys(value).length > 0) {
    return <Fields fields={Object.fromEntries(Object.entries(value))} />;
  }
  return <pre className={styles.literal}>{cellText(value)}</pre>;
}
