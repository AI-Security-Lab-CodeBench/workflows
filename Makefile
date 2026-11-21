.PHONY: remote-up
remote-up:
	bash scripts/remote-up.sh

.PHONY: remote-prepare
remote-prepare:
	bash scripts/remote-up.sh prepare

DATASET ?= cloud_infrastructure_commands_grounded_v1
export DATASET STAGE
.PHONY: stages materialize-through materialize-unsynced
stages:
	bash scripts/remote-up.sh stages
materialize-through:
	bash scripts/remote-up.sh through
materialize-unsynced:
	bash scripts/remote-up.sh unsynced

.PHONY: e2e
e2e:
	$(UV) run --no-project --with-requirements tests/e2e/requirements.txt python -m pytest -q -s --tb=line -p no:cacheprovider tests/compose_e2e.py

PYTHON ?= python3
UV ?= uv

ifeq ($(shell test -w . || echo readonly),readonly)
export UV_PROJECT_ENVIRONMENT ?= $(HOME)/.cache/codebench/lint-venv
endif

.PHONY: lint static-checks
lint: static-checks
	$(PYTHON) -B -m unittest discover -s tests -p test_asset_boundary.py

static-checks:
	$(PYTHON) -B -m scripts.asset_boundary
	$(UV) run --locked ruff check --no-cache codebench_pipeline
	$(UV) run --locked basedpyright

.PHONY: test-suite
test-suite:
	timeout --verbose --signal=KILL 600s $(PYTHON) -B scripts/test-suite
