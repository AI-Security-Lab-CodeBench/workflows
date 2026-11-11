import type {ReactNode} from 'react';
import {ApiError, failure} from './api';
import styles from './ui.module.css';

export function Status({children, error}: {readonly children?: ReactNode; readonly error?: unknown}) {
  return <div className={styles.status} role={error === undefined ? 'status' : 'alert'}>
    {error instanceof ApiError && error.code === 'not_materialized' ? 'Not materialized. ' : ''}
    {error instanceof ApiError && (error.code === 'stale' || error.code === 'conflict')
      ? 'Stale or conflicting state. Refresh explicitly to recover. ' : ''}
    {error instanceof ApiError && error.code === 'not_ready' ? 'Preparation required. ' : ''}
    {error instanceof ApiError && error.code === 'continuation_failed'
      ? 'Continuation failed. Refresh saved review state before explicitly retrying. ' : ''}
    {error === undefined ? children : failure(error)}
  </div>;
}
