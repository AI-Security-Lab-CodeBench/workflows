.PHONY: remote-up
remote-up:
	bash scripts/remote-up.sh

.PHONY: e2e
e2e:
	timeout --verbose --kill-after=10s 290s bash scripts/e2e.sh

PYTHON ?= python3
UV ?= uv

ifeq ($(shell test -w . || echo readonly),readonly)
export UV_PROJECT_ENVIRONMENT ?= $(HOME)/.cache/codebench/lint-venv
endif

.PHONY: lint
lint:
	$(PYTHON) -B -m unittest discover -s tests -p test_asset_boundary.py
	$(PYTHON) -B -m scripts.asset_boundary
	$(UV) run --locked ruff check --no-cache codebench_pipeline
	$(UV) run --locked basedpyright

.PHONY: test-suite
test-suite:
	timeout --verbose --signal=KILL 600s $(PYTHON) -B scripts/test-suite
