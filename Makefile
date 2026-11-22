REPO_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PYTHON ?= python3
UV ?= uv

export E2E_NODE
.PHONY: e2e
e2e:
	$(PYTHON) -B "$(REPO_DIR)/scripts/stack.py" e2e "$$E2E_NODE"

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
