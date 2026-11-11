import type {Query} from './models';

export function toggleSort(query: Query, field: string): Query {
  const existing = query.sort.find((sort) => sort.field === field);
  const remaining = query.sort.filter((sort) => sort.field !== field);
  if (existing?.direction === 'desc') {return {...query, offset: 0, sort: remaining};}
  const sort: Query['sort'][number] = {field, direction: existing ? 'desc' : 'asc'};
  return {...query, offset: 0, sort: existing ?
    query.sort.map((entry) => entry.field === field ? sort : entry) :
    [...remaining.slice(0, 3), sort]};
}
