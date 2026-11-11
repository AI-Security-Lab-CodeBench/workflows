import logging
from collections.abc import Sequence
from typing import Final, assert_never

from pydantic import BaseModel, ValidationError
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.exceptions import HTTPException
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from codebench_pipeline.data_view_models import DatasetQueryRequest, DatasetViewError
from codebench_pipeline.data_views import DatasetDataService
from codebench_pipeline.human_review import HumanReviewError
from codebench_pipeline.review_api_models import (
    CompletionRequest,
    DecisionRequest,
    EvidenceTarget,
    PublicationTarget,
    RetractionRequest,
    ReviewBackend,
)
from codebench_pipeline.review_stages import ReviewRegistration, ReviewStageList, annotation_stage
from codebench_pipeline.table_query import TableQueryError

MAX_BODY_BYTES: Final = 64 * 1024


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse({"error": {"code": code, "message": message}}, status_code=status)


def _json(model: BaseModel) -> Response:
    return Response(model.model_dump_json(), media_type="application/json")


async def _body[T: BaseModel](request: Request, model: type[T]) -> T:
    content_types = request.headers.getlist("content-type")
    if len(content_types) != 1 or content_types[0].split(";", 1)[0].strip().lower() != "application/json":
        raise HTTPException(422)
    if request.query_params:
        raise HTTPException(422)
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_BODY_BYTES:
            raise HTTPException(413)
        body.extend(chunk)
    return model.model_validate_json(bytes(body))


def _target[T: BaseModel](request: Request, model: type[T]) -> T:
    if tuple(request.query_params.keys()) != ("dataset_id",):
        raise HTTPException(422)
    if len(request.query_params.getlist("dataset_id")) != 1:
        raise HTTPException(422)
    path = {key: value for key, value in request.path_params.items() if key != "review_stage"}
    return model.model_validate({"dataset_id": request.query_params["dataset_id"], **path})


async def _http_error(_request: Request, error: Exception) -> Response:
    match error:
        case HTTPException(status_code=404):
            return _error(404, "not_found", "API resource not found")
        case HTTPException(status_code=413):
            return _error(413, "invalid_request", "Request body exceeds 64 KiB")
        case HTTPException(status_code=status):
            return _error(status, "invalid_request", "Invalid API request")
        case _:
            return _error(500, "internal_error", "API request failed")


def make_dataset_api(
    data: DatasetDataService,
    review: ReviewBackend,
    port: int,
    *,
    stages: Sequence[ReviewRegistration] = (),
) -> Starlette:
    if not 1 <= port <= 65535:
        raise ValueError("API port must be between 1 and 65535")
    allowed_hosts = {f"localhost:{port}", f"127.0.0.1:{port}"}
    registered = (ReviewRegistration(annotation_stage(), review), *stages)
    registry = {entry.stage.stage_id: entry for entry in registered}
    if len(registry) != len(registered):
        raise ValueError("Review stage IDs must be unique")

    def backend(request: Request) -> ReviewBackend:
        stage_id = request.path_params.get("review_stage", "annotations")
        if stage_id not in registry:
            raise HTTPException(404)
        return registry[stage_id].backend

    async def boundary(request: Request, call_next: RequestResponseEndpoint) -> Response:
        hosts = request.headers.getlist("host")
        origins = request.headers.getlist("origin")
        if (
            len(hosts) != 1
            or hosts[0] not in allowed_hosts
            or (request.method != "GET" and (len(origins) != 1 or origins[0] != f"http://{hosts[0]}"))
        ):
            response = _error(403, "forbidden", "Request host or origin is not allowed")
        else:
            try:
                response = await call_next(request)
            except (ValidationError, TableQueryError):
                response = _error(422, "invalid_request", "Invalid API request")
            except DatasetViewError as error:
                match error.code:
                    case "not_ready":
                        response = _error(
                            409,
                            error.code,
                            error.detail,
                        )
                    case "not_found":
                        response = _error(404, error.code, "Dataset or stage not found.")
                    case "not_materialized":
                        response = _error(404, error.code, "This stage has no persisted output.")
                    case _:
                        assert_never(error.code)
            except HumanReviewError as error:
                match error.code:
                    case "not_found":
                        response = _error(404, error.code, "Review batch or record not found.")
                    case "conflict":
                        response = _error(
                            409, error.code, "Another review operation is in progress. Refresh and retry."
                        )
                    case "stale":
                        response = _error(
                            409, error.code, "The snapshot or decision changed. Refresh before saving."
                        )
                    case "pending":
                        response = _error(
                            409, error.code, "Decide every pending record before completing review."
                        )
                    case "ineligible":
                        response = _error(
                            409, error.code, "Human approval cannot override this record's eligibility."
                        )
                    case "selection":
                        response = _error(
                            409, error.code, "Approved records do not satisfy this stage's selection bounds."
                        )
                    case "completed":
                        response = _error(
                            409, error.code, "This review is completed. Reopen it before editing."
                        )
                    case "publication_failed":
                        response = _error(
                            500, error.code, "Publication failed. Refresh the review to recover."
                        )
                    case "continuation_failed":
                        response = _error(
                            500,
                            error.code,
                            "Pipeline continuation failed. Inspect the run, refresh, and retry.",
                        )
                    case _:
                        assert_never(error.code)
            except Exception as error:
                logging.getLogger("uvicorn.error").error(
                    "CodeBench API request failed: %s", type(error).__name__
                )
                response = _error(500, "internal_error", "API request failed")
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        return response

    async def datasets(request: Request) -> Response:
        if request.query_params:
            raise HTTPException(422)
        return _json(await run_in_threadpool(data.datasets))

    async def review_stages(request: Request) -> Response:
        if request.query_params:
            raise HTTPException(422)
        return _json(ReviewStageList(stages=tuple(entry.stage for entry in registered)))

    async def table(request: Request) -> Response:
        query = await _body(request, DatasetQueryRequest)
        return _json(await run_in_threadpool(data.query, request.path_params["stage"], query))

    async def view(request: Request) -> Response:
        query = await _body(request, DatasetQueryRequest)
        await run_in_threadpool(data.specification, query.dataset_id)
        return _json(await run_in_threadpool(backend(request).view, query))

    async def decide(request: Request) -> Response:
        change = await _body(request, DecisionRequest)
        await run_in_threadpool(data.specification, change.dataset_id)
        return _json(await run_in_threadpool(backend(request).decide, change))

    async def complete(request: Request) -> Response:
        expected = await _body(request, CompletionRequest)
        await run_in_threadpool(data.specification, expected.dataset_id)
        return _json(await run_in_threadpool(backend(request).complete, expected))

    async def reopen(request: Request) -> Response:
        expected = await _body(request, CompletionRequest)
        await run_in_threadpool(data.specification, expected.dataset_id)
        return _json(await run_in_threadpool(backend(request).reopen, expected))

    async def retract(request: Request) -> Response:
        change = await _body(request, RetractionRequest)
        await run_in_threadpool(data.specification, change.dataset_id)
        return _json(await run_in_threadpool(backend(request).retract, change))

    async def continue_review(request: Request) -> Response:
        expected = await _body(request, CompletionRequest)
        await run_in_threadpool(data.specification, expected.dataset_id)
        return _json(await run_in_threadpool(backend(request).continue_review, expected))

    async def evidence(request: Request) -> Response:
        target = _target(request, EvidenceTarget)
        await run_in_threadpool(data.specification, target.dataset_id)
        return _json(await run_in_threadpool(backend(request).evidence, target))

    async def download(request: Request) -> Response:
        target = _target(request, PublicationTarget)
        await run_in_threadpool(data.specification, target.dataset_id)
        payload = await run_in_threadpool(review.download, target)
        return Response(
            payload,
            media_type="text/csv; charset=utf-8",
            headers={"Content-Disposition": 'attachment; filename="commands.csv"'},
        )

    return Starlette(
        routes=[
            Route("/datasets", datasets, methods=["GET"]),
            Route("/review-stages", review_stages, methods=["GET"]),
            Route("/data/{stage:path}", table, methods=["POST"]),
            Route("/review", view, methods=["POST"]),
            Route("/review/decision", decide, methods=["PUT"]),
            Route("/review/complete", complete, methods=["POST"]),
            Route("/review/reopen", reopen, methods=["POST"]),
            Route("/review/retract", retract, methods=["POST"]),
            Route("/review/continue", continue_review, methods=["POST"]),
            Route("/review/{batch_id}/evidence/{candidate_id}", evidence, methods=["GET"]),
            Route("/reviews/{review_stage}", view, methods=["POST"]),
            Route("/reviews/{review_stage}/decision", decide, methods=["PUT"]),
            Route("/reviews/{review_stage}/complete", complete, methods=["POST"]),
            Route("/reviews/{review_stage}/reopen", reopen, methods=["POST"]),
            Route("/reviews/{review_stage}/retract", retract, methods=["POST"]),
            Route("/reviews/{review_stage}/continue", continue_review, methods=["POST"]),
            Route("/reviews/{review_stage}/{batch_id}/evidence/{candidate_id}", evidence, methods=["GET"]),
            Route("/publications/{publication_id}/commands.csv", download, methods=["GET"]),
        ],
        middleware=[Middleware(BaseHTTPMiddleware, dispatch=boundary)],
        exception_handlers={HTTPException: _http_error},
    )
