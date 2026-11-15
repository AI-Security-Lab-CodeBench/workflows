import type {Query} from './models';

export function artifactSelectionLink(search: string, mode: 'output' | 'inputs', input?: readonly string[]) {
  const query = new URLSearchParams(search);
  query.set('data_view', mode);
  query.delete('data_input');
  query.delete('input_partition');
  if (mode === 'inputs' && input) {query.set('data_input', JSON.stringify(input));}
  return `?${query}`;
}

export function artifactPartitionLink(search: string, parameter: string, partition: string | null) {
  const query = new URLSearchParams(search);
  if (partition === null) {query.delete(parameter);}
  else {query.set(parameter, partition);}
  return `?${query}`;
}

export function toggleSort(query: Query, field: string): Query {
  const existing = query.sort.find((sort) => sort.field === field);
  const remaining = query.sort.filter((sort) => sort.field !== field);
  if (existing?.direction === 'desc') {return {...query, offset: 0, sort: remaining};}
  const sort: Query['sort'][number] = {field, direction: existing ? 'desc' : 'asc'};
  return {...query, offset: 0, sort: existing ?
    query.sort.map((entry) => entry.field === field ? sort : entry) :
    [...remaining.slice(0, 3), sort]};
}
