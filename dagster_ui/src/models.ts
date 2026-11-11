import {z} from 'zod';

const text = z.string().min(1);
const revision = z.number().int().nonnegative();
const digest = z.string().regex(/^[0-9a-f]{64}$/);
export const decisionValue = z.enum(['pending', 'approve', 'reject']);
export type DecisionValue = z.infer<typeof decisionValue>;
export const querySchema = z.object({
  offset: revision,
  limit: z.number().int().min(1).max(200),
  search: z.string().max(2000),
  filters: z.array(z.discriminatedUnion('operator', [
    z.object({field: text.max(128), operator: z.literal('contains'), value: z.string().max(8192)}),
    z.object({field: text.max(128), operator: z.literal('equals'), value: z.string().max(8192)}),
    z.object({field: text.max(128), operator: z.literal('is_null')}),
  ])).max(20),
  sort: z.array(z.object({field: text.max(128), direction: z.enum(['asc', 'desc'])})).max(4),
});
export type Query = z.infer<typeof querySchema>;
export const initialQuery = querySchema.parse({offset: 0, limit: 25, search: '', filters: [], sort: []});
export const rowSchema = z.object({row_index: revision, values: z.record(z.string(), z.json())});
export const pageSchema = z.object({
  columns: z.array(z.string()), total_rows: revision, matched_rows: revision,
  offset: revision, limit: z.number().int().positive(), rows: z.array(rowSchema),
});
export type Row = z.infer<typeof rowSchema>;
export type Page = z.infer<typeof pageSchema>;
export const datasetsSchema = z.object({
  datasets: z.array(z.object({dataset_id: text, category_id: text, category: text})),
});
export type Dataset = z.infer<typeof datasetsSchema>['datasets'][number];
export const reviewStagesSchema = z.object({
  stages: z.array(z.object({
    stage_id: text, title: text, asset_keys: z.array(z.array(text).min(1)),
    completion_label: text, min_approvals: revision, max_approvals: revision.nullable(),
    can_continue: z.boolean().default(false),
    continuation_label: text.default('Continue pipeline'),
    additional_publication_stages: z.array(text).default([]),
  })),
});
export type ReviewStage = z.infer<typeof reviewStagesSchema>['stages'][number];
const binding = {dataset_id: text, stage: text, snapshot_id: digest};
export const dataSchema = z.discriminatedUnion('kind', [
  z.object({...binding, kind: z.literal('table'), page: pageSchema,
    publication_state: z.enum(['published', 'legacy_unreviewed', 'stale']).nullable().optional()}),
  z.object({...binding, kind: z.literal('rules'), rules: z.json()}),
]);
const version = {
  batch_id: text, generation: z.number().int().positive(), batch_revision: revision,
};
const tokenSchema = z.object({
  ...version, dataset_id: text, snapshot_id: digest, decision_digest: digest, token: text,
  review_stage: text.default('annotations'),
});
export const batchSchema = z.object({
  ...version,
  snapshot: z.object({
    dataset_id: text, snapshot_id: digest,
    review_stage: text.default('annotations'),
    candidate_ids: z.array(text), eligible_candidate_ids: z.array(text),
  }),
  state: z.enum(['open', 'completed', 'superseded']),
  created_at: text, updated_at: text, reviewer: z.string(),
  completion: tokenSchema.nullable(),
  continuation_run_id: text.nullable().default(null),
});
export const countsSchema = z.object({pending: revision, approve: revision, reject: revision});
export const decisionSchema = z.object({
  batch_id: text, candidate_id: text, decision: decisionValue, notes: z.string(),
  reviewer: z.string(), row_revision: revision, created_at: text, updated_at: text,
});
export const publicationSchema = z.object({
  payload_key: text, payload_hash: digest, csv_reference: text, bundle_reference: text,
  dagster_run_id: text, publication_id: text, authorization: tokenSchema, created_at: text,
});
export const reviewSchema = z.object({
  batch: batchSchema, counts: countsSchema, decisions: z.array(decisionSchema),
  page: pageSchema.extend({rows: z.array(rowSchema.extend({review_record_id: text}))}),
  publication: publicationSchema.nullable(),
});
export const savedSchema = z.object({
  batch: batchSchema, decision: decisionSchema, counts: countsSchema,
});
export const completeSchema = z.object({
  batch: batchSchema, counts: countsSchema, completion: tokenSchema,
  publication: publicationSchema.nullable(), run_id: text.nullable(),
});
export const continuationSchema = completeSchema.extend({
  completion: tokenSchema.nullable().default(null),
  publication: publicationSchema.nullable().default(null),
  run_id: text.nullable().default(null),
});
export const reopenSchema = z.object({batch: batchSchema, counts: countsSchema});
export const evidenceSchema = z.object({fields: z.record(z.string(), z.json())});
export type Review = z.infer<typeof reviewSchema>;
export type Decision = z.infer<typeof decisionSchema>;
export type Batch = z.infer<typeof batchSchema>;
export type Publication = z.infer<typeof publicationSchema>;
export type ReviewRow = Review['page']['rows'][number];

export function stageResponses(stageId: string, additionalPublicationStages: readonly string[] = []) {
  const matches = (batch: Batch) => batch.snapshot.review_stage === stageId &&
    (!batch.completion || batch.completion.review_stage === stageId);
  const matchesPublication = (publication: Publication | null) => !publication ||
    publication.authorization.review_stage === stageId ||
    additionalPublicationStages.includes(publication.authorization.review_stage);
  return {
    review: reviewSchema.refine((data) => matches(data.batch) &&
      matchesPublication(data.publication)),
    saved: savedSchema.refine((data) => matches(data.batch)),
    complete: completeSchema.refine((data) => matches(data.batch) &&
      data.completion.review_stage === stageId &&
      matchesPublication(data.publication)),
    continue: continuationSchema.refine((data) => matches(data.batch) &&
      (!data.completion || data.completion.review_stage === stageId) &&
      matchesPublication(data.publication)),
    reopen: reopenSchema.refine((data) => matches(data.batch)),
  };
}
