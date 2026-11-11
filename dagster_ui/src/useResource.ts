import {useEffect, useRef, useState} from 'react';
import type {z} from 'zod';
import {ApiError, request} from './api';
import type {Query} from './models';

type Result<T> = {readonly key: string; readonly data?: T; readonly error?: unknown};

export function useResource<T>(
  path: string, schema: z.ZodType<T>, body: unknown, refresh: number, method: 'GET' | 'POST' = 'POST',
) {
  const key = JSON.stringify([path, body, refresh]);
  const [result, setResult] = useState<Result<T>>();
  useEffect(() => {
    const controller = new AbortController();
    void request(path, schema, method, body, controller.signal).then(
      (data) => {if (!controller.signal.aborted) {setResult({key, data});}},
      (error: unknown) => {if (!controller.signal.aborted) {setResult({key, error});}},
    );
    return () => controller.abort();
  }, [key, path, schema, method, body]);
  return result?.key === key ? result : undefined;
}

export function useSnapshotQuery<T>(
  path: string, schema: z.ZodType<T>, datasetId: string, query: Query,
  getSnapshot: (data: T) => string,
) {
  const snapshot = useRef<string | null>(null);
  const selector = useRef(getSnapshot);
  selector.current = getSnapshot;
  const active = useRef<AbortController>();
  const [refresh, setRefresh] = useState(0);
  const key = JSON.stringify([path, datasetId, query, refresh]);
  const [received, setReceived] = useState<Result<T>>();
  useEffect(() => {
    const controller = new AbortController();
    active.current = controller;
    void request(path, schema, 'POST',
      {dataset_id: datasetId, snapshot_id: snapshot.current, query}, controller.signal).then(
      (data) => {
        if (!controller.signal.aborted) {
          snapshot.current = selector.current(data);
          setReceived({key, data});
        }
      },
      (error: unknown) => {if (!controller.signal.aborted) {setReceived({key, error});}},
    );
    return () => controller.abort();
  }, [key, path, schema, datasetId, query, refresh]);
  const result = received?.key === key ? received : undefined;
  return {
    data: result?.data, error: result?.error, loading: !result,
    stale: result?.error instanceof ApiError && result.error.status === 409,
    refresh() {
      active.current?.abort();
      snapshot.current = null;
      setRefresh((value) => value + 1);
    },
    reload() {active.current?.abort(); setRefresh((value) => value + 1);},
  };
}
