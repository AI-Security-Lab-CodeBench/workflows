from __future__ import annotations

import json
from typing import Final, assert_never

from psycopg import sql
from pydantic import JsonValue, TypeAdapter

from codebench_pipeline.data_view_models import DatasetQueryRequest
from codebench_pipeline.human_review import (
    HumanReviewError,
    PreparedReview,
    ReviewBatch,
    ReviewDecision,
    ReviewPartition,
)
from codebench_pipeline.human_review_schema import ReviewRepository
from codebench_pipeline.review_api_models import (
    EvidenceTarget,
    NextReviewRequest,
    ReviewItemResponse,
    ReviewPage,
    ReviewQueryRequest,
    ReviewTableRow,
    ReviewViewResponse,
)
from codebench_pipeline.review_postgres_values import decision_values, number_key
from codebench_pipeline.table_query import NullFilter, TableQuery, TableQueryError, TextFilter

HUMAN_COLUMNS: Final = ("human_decision", "human_reviewer", "human_notes", "human_edits")
REPRESENTATIVES: Final = """WITH occurrences AS (
    SELECT r.*, count(*) OVER(PARTITION BY record_hash) AS occurrence_count,
        max(blocked_reason) OVER(PARTITION BY record_hash) AS item_blocked_reason
    FROM review_rows r WHERE batch_id=%s
), representatives AS (
    SELECT DISTINCT ON(record_hash) * FROM occurrences ORDER BY record_hash,row_index
), items AS (
    SELECT r.*, i.data AS decision_data, r.cells || i.cells AS query_cells
    FROM representatives r JOIN review_items i USING(batch_id,record_hash)
) """


class ReviewQueries(ReviewRepository):
    def partitions(
        self, stage: str, *, artifact_id: str | None = None, scope_id: str | None = None,
    ) -> tuple[ReviewPartition, ...]:
        rows = self.connection.execute(
            """SELECT dataset_id,artifact_id,scope_id,invalidated IS NOT NULL AS invalidated,
                CASE WHEN artifact_id='' THEN dataset_id
                    ELSE data->'snapshot'->>'partition_key' END AS partition_key
                FROM review_batches WHERE review_stage=%s AND current AND prepared IS NOT NULL
                AND (%s::text IS NULL OR artifact_id=%s) AND (%s::text IS NULL OR scope_id=%s)
                ORDER BY artifact_id,scope_id,dataset_id""",
            (stage, artifact_id, artifact_id, scope_id, scope_id),
        )
        return tuple(ReviewPartition.model_validate(row) for row in rows)

    def _metadata(self, batch_id: str) -> dict[str, JsonValue] | None:
        row = self.connection.execute(
            "SELECT prepared FROM review_batches WHERE batch_id=%s", (batch_id,)
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "Review batch not found")
        return TypeAdapter(dict[str, JsonValue] | None).validate_python(row["prepared"])

    def prepared(self, batch_id: str) -> PreparedReview:
        metadata = self._metadata(batch_id)
        if metadata is None:
            raise HumanReviewError("not_found", "Review table has not been prepared")
        rows = tuple(TypeAdapter(dict[str, JsonValue]).validate_json(
            TypeAdapter(str).validate_python(row["row_data"])
        ) for row in self.connection.execute(
            "SELECT row_data FROM review_rows WHERE batch_id=%s ORDER BY row_index", (batch_id,)
        ))
        return PreparedReview.model_validate_json(json.dumps({
            **metadata, "snapshot": self._full_batch(batch_id).snapshot.model_dump(mode="json"),
            "rows": rows,
        }))

    def validation(self, batch_id: str) -> dict[str, JsonValue]:
        metadata = self._metadata(batch_id)
        if metadata is None:
            return {}
        return TypeAdapter(dict[str, JsonValue]).validate_python(metadata["validation"])

    def column_schema(self, batch_id: str) -> dict[str, dict[str, JsonValue]]:
        metadata = self._metadata(batch_id)
        if metadata is None:
            return {}
        return TypeAdapter(dict[str, dict[str, JsonValue]]).validate_python(metadata.get("column_schema", {}))

    def rows_for_hash(self, batch_id: str, candidate_id: str) -> tuple[dict[str, JsonValue], ...]:
        rows = self.connection.execute(
            """SELECT row_data FROM review_rows WHERE batch_id=%s AND record_hash=(
                SELECT record_hash FROM review_rows WHERE batch_id=%s AND candidate_id=%s)
                ORDER BY row_index""", (batch_id, batch_id, candidate_id)
        ).fetchall()
        if not rows:
            raise HumanReviewError("not_found", "Candidate is not in this batch")
        return tuple(TypeAdapter(dict[str, JsonValue]).validate_json(
            TypeAdapter(str).validate_python(row["row_data"])
        ) for row in rows)

    def _query_batch(self, request: DatasetQueryRequest, stage: str) -> tuple[ReviewBatch, tuple[str, ...]]:
        request = ReviewQueryRequest.model_validate(request.model_dump(exclude={"after_id"}))
        batch = self.get_current_batch(
            request.dataset_id, stage, compact=True, artifact_id=request.artifact_id, scope_id=request.scope_id
        )
        if request.snapshot_id is not None and request.snapshot_id != batch.snapshot.snapshot_id:
            raise HumanReviewError("stale", "Review snapshot changed; refresh before continuing")
        metadata = self._metadata(batch.batch_id)
        if metadata is None:
            raise HumanReviewError("not_found", "Review table has not been prepared")
        columns = TypeAdapter(tuple[str, ...]).validate_json(json.dumps(metadata["columns"]))
        return batch, columns + HUMAN_COLUMNS

    def _clauses(
        self, query: TableQuery, columns: tuple[str, ...],
    ) -> tuple[sql.Composable, list[str | int], sql.Composable]:
        for clause in (*query.filters, *query.sort):
            if clause.field not in columns:
                raise TableQueryError(clause.field, f"Unknown column: {clause.field}")
        if len({clause.field for clause in query.sort}) != len(query.sort):
            raise TableQueryError("sort", "Sort columns must be distinct")
        clauses: list[sql.Composable] = [sql.SQL("TRUE")]
        params: list[str | int] = []
        if query.search:
            clauses.append(sql.SQL(
                "EXISTS(SELECT 1 FROM jsonb_each(query_cells) c WHERE strpos(c.value->>'fold',%s)>0)"
            ))
            params.append(query.search.casefold())
        for condition in query.filters:
            cell = sql.SQL("(query_cells->{})").format(sql.Literal(condition.field))
            match condition:
                case NullFilter():
                    clauses.append(sql.SQL("({}->>'kind')::int=4").format(cell))
                case TextFilter():
                    match condition.operator:
                        case "contains":
                            clauses.append(sql.SQL("strpos({}->>'fold',%s)>0").format(cell))
                            params.append(condition.value.casefold())
                        case "equals":
                            number = number_key(condition.value)
                            if number is not None:
                                clauses.append(sql.SQL(
                                    "CASE WHEN ({c}->>'kind')::int=1 THEN {c}->>'number_key'=%s "
                                    "ELSE {c}->>'text'=%s END"
                                ).format(c=cell))
                                params.extend((number, condition.value))
                            else:
                                clauses.append(sql.SQL("({}->>'kind')::int<>1 AND {}->>'text'=%s").format(cell, cell))
                                params.append(condition.value)
                        case _:
                            assert_never(condition.operator)
                case _:
                    assert_never(condition)
        order: list[sql.Composable] = []
        for ordering in query.sort:
            cell = sql.SQL("(query_cells->{})").format(sql.Literal(ordering.field))
            direction = sql.SQL("ASC" if ordering.direction == "asc" else "DESC")
            order.extend((
                sql.SQL("NULLIF(({c}->>'kind')::int,4) {d} NULLS LAST").format(c=cell, d=direction),
                sql.SQL("({c}->>'number')::numeric {d} NULLS LAST").format(c=cell, d=direction),
                sql.SQL("({c}->>'sort') COLLATE \"C\" {d} NULLS LAST").format(c=cell, d=direction),
            ))
        order.append(sql.SQL("row_index ASC"))
        return sql.SQL(" AND ").join(clauses), params, sql.SQL(",").join(order)

    def _item_response(
        self, row: dict[str, JsonValue], batch: ReviewBatch,
    ) -> ReviewItemResponse:
        decision = ReviewDecision.model_validate(row["decision_data"])
        values = TypeAdapter(dict[str, JsonValue]).validate_json(
            TypeAdapter(str).validate_python(row["row_data"])
        )
        values.update(decision_values(decision))
        return ReviewItemResponse(
            row=ReviewTableRow(
                row_index=TypeAdapter(int).validate_python(row["row_index"]),
                values=values, review_record_id=decision.candidate_id,
                occurrence_count=TypeAdapter(int).validate_python(row["occurrence_count"]),
                blocked_reason=TypeAdapter(str | None).validate_python(row["item_blocked_reason"]),
            ),
            decision=decision, review_columns=batch.snapshot.review_columns,
            column_schema=self.column_schema(batch.batch_id),
        )

    def review_page(self, request: DatasetQueryRequest, stage: str) -> ReviewViewResponse:
        batch, columns = self._query_batch(request, stage)
        where, params, order = self._clauses(request.query, columns)
        count = next(self.connection.execute(
            sql.SQL(REPRESENTATIVES + "SELECT count(*) AS total,count(*) FILTER(WHERE {}) AS matched FROM items").format(where),
            [batch.batch_id, *params],
        ))
        records = self.connection.execute(
            sql.SQL(REPRESENTATIVES + "SELECT * FROM items WHERE {} ORDER BY {} LIMIT %s OFFSET %s").format(where, order),
            [batch.batch_id, *params, request.query.limit, request.query.offset],
        )
        items = tuple(self._item_response(row, batch) for row in records)
        publication = self.last_publication(
            request.dataset_id, stage,
            artifact_id=batch.snapshot.artifact_id, scope_id=batch.snapshot.scope_id,
        )
        if publication is not None and publication.authorization != batch.completion:
            publication = None
        return ReviewViewResponse(
            batch=batch, counts=self.counts(batch.batch_id), decisions=tuple(item.decision for item in items),
            page=ReviewPage(columns=columns, total_rows=TypeAdapter(int).validate_python(count["total"]),
                            matched_rows=TypeAdapter(int).validate_python(count["matched"]),
                            offset=request.query.offset, limit=request.query.limit, rows=tuple(item.row for item in items)),
            publication=publication, review_columns=batch.snapshot.review_columns,
            column_schema=self.column_schema(batch.batch_id),
        )

    def item(self, target: EvidenceTarget) -> ReviewItemResponse:
        batch = self.get_batch(target.batch_id, compact=True)
        if (batch.snapshot.dataset_id, batch.snapshot.artifact_id, batch.snapshot.scope_id) != (
            target.dataset_id, target.artifact_id, target.scope_id
        ):
            raise HumanReviewError("not_found", "Batch does not belong to this dataset")
        row = self.connection.execute(
            sql.SQL(REPRESENTATIVES + """SELECT * FROM items WHERE record_hash=(
                SELECT record_hash FROM review_rows WHERE batch_id=%s AND candidate_id=%s)"""),
            (target.batch_id, target.batch_id, target.candidate_id),
        ).fetchone()
        if row is None:
            raise HumanReviewError("not_found", "Candidate is not in this batch")
        return self._item_response(row, batch)

    def next_pending(self, request: NextReviewRequest, stage: str) -> ReviewItemResponse | None:
        batch, columns = self._query_batch(request, stage)
        where, params, order = self._clauses(request.query, columns)
        query = sql.SQL(REPRESENTATIVES + """, ranked AS (
            SELECT *,row_number() OVER(ORDER BY {}) AS position, ({}) AS matches FROM items
        ) SELECT * FROM ranked WHERE matches AND decision_data->>'decision'='pending'
            AND (%s::text IS NULL OR position > coalesce((
                SELECT position FROM ranked WHERE record_hash=(
                    SELECT record_hash FROM review_rows WHERE batch_id=%s AND candidate_id=%s)
            ),0)) ORDER BY position LIMIT 1""").format(order, where)
        row = self.connection.execute(
            query, [batch.batch_id, *params, request.after_id, batch.batch_id, request.after_id]
        ).fetchone()
        return self._item_response(row, batch) if row else None
