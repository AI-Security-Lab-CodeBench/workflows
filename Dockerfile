FROM docker.io/library/python:3.13-slim-bookworm@sha256:a5a7017efc5969e00ae2b91c8eb0631049a569652aa4ffc4501769049ae45c72 AS python
FROM ghcr.io/astral-sh/uv:0.11.14@sha256:1025398289b62de8269e70c45b91ffa37c373f38118d7da036fb8bb8efc85d97 AS uv

FROM docker.io/library/golang:1.26.0-bookworm@sha256:2a0ba12e116687098780d3ce700f9ce3cb340783779646aafbabed748fa6677c AS bashguard
WORKDIR /src
COPY bashguard/go.mod bashguard/go.sum ./
COPY bashguard/internal/analyze ./internal/analyze
COPY bashguard/cmd/bashguard-audit ./cmd/bashguard-audit
RUN CGO_ENABLED=0 GOTOOLCHAIN=local go build -mod=readonly -trimpath -o /out/bashguard-audit ./cmd/bashguard-audit

FROM python AS dependencies
WORKDIR /opt/codebench
COPY --from=uv /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
COPY tests/e2e/requirements.txt /tmp/qa-requirements.txt
RUN uv sync --locked --no-dev --no-install-project --no-python-downloads \
    && uv pip install --no-deps -r /tmp/qa-requirements.txt iniconfig==2.3.0 pluggy==1.6.0 \
    && uv pip check

FROM docker.io/library/node:22-bookworm@sha256:e13726ca604184d7a91fcacab1fc09eefd642dc28e79ac34e85540dd93b78cfb AS native-ui
WORKDIR /opt/codebench
COPY --from=python /usr/local /usr/local
COPY --from=dependencies /opt/codebench/.venv ./.venv
COPY codebench_pipeline/ui_build.py ./codebench_pipeline/ui_build.py
COPY dagster_ui ./dagster_ui
RUN .venv/bin/python -m codebench_pipeline.ui_build

FROM python AS runtime
WORKDIR /opt/codebench
ENV PATH="/opt/codebench/.venv/bin:/opt/codebench/bin:${PATH}" \
    CODEBENCH_HOME=/var/lib/codebench/datasets \
    DAGSTER_HOME=/var/lib/codebench/datasets/dagster \
    HOME=/tmp \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
RUN useradd --uid 1000 --gid 100 --home-dir /tmp --no-create-home codebench \
    && install -d -o 1000 -g 100 "$CODEBENCH_HOME" "$CODEBENCH_HOME/dagster"
COPY --from=uv /uv /usr/local/bin/uv
COPY --from=dependencies /opt/codebench/.venv ./.venv
COPY --from=bashguard /out/bashguard-audit ./bin/bashguard-audit
COPY --from=native-ui /opt/codebench/dagster_ui/build ./dagster_ui/build
COPY pyproject.toml uv.lock ./
COPY codebench_pipeline ./codebench_pipeline
COPY business-configs ./business-configs
COPY bashguard/rules ./bashguard/rules
COPY scripts ./scripts
COPY transforms ./transforms
COPY tests ./tests
COPY deploy/workspace.yaml deploy/dagster.yaml ./config/
COPY --chown=1000:100 deploy/dagster.yaml /var/lib/codebench/datasets/dagster/dagster.yaml
USER 1000:100
