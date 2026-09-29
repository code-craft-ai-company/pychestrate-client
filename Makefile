# Pychestrate pod client -- developer and packaging entry points.
#
# Run 'make' (or 'make help') for the list of targets. Every target that
# appears in the help carries a '## description' on its rule line, and
# '##@ Section' lines group them; add both when you add a target.
#
# Tools come from the repository virtual environment ($(VENV), created by
# 'make setup'), never from whatever python is first on PATH.
#
# pychestrate-client is proprietary: dist-publish uploads only to a
# private index that poetry knows by name, and only with CONFIRM=1.
# (pyproject.toml also carries the 'Private :: Do Not Upload'
# classifier, which PyPI rejects.)

.DEFAULT_GOAL := help
SHELL := /bin/bash

VENV     ?= .venv
PYTHON   := $(VENV)/bin/python
TESTBASE := $(VENV)/bin/testbase
POETRY   ?= poetry

# The test root's top-level package and the client's test suite.
TEST_ROOT      ?= source/testroots/ccai
TEST_NAMESPACE := $(notdir $(TEST_ROOT))
TEST_SUITE     := $(TEST_NAMESPACE).tests.pychestrate.client

# The environment installs with --no-root, so the client package is not on
# the venv's path: run code and tests straight from the source tree.
SOURCE_PYTHONPATH := source/packages:source/testroots

.PHONY: help setup test lint dist-clean dist-build dist-publish

##@ Help

help: ## Show this list of targets (the default)
	@echo "usage: make <target> [VAR=value ...]"
	@awk 'BEGIN {FS = ":.*## "} \
	     /^##@ / {printf "\n%s\n", substr($$0, 5)} \
	     /^[a-zA-Z0-9_-]+:.*## / {printf "  %-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo

##@ Development

setup: ## Create or refresh .venv with poetry (development/setup-environment)
	./development/setup-environment

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
#
# testbase has two silent traps, both avoided here (and flagged by 'make lint'):
#
#   * An --includes value is a string prefix, so it also runs every module
#     whose name merely starts with it.
#   * --excludes does nothing: no test is ever excluded.
#
# development/select-tests turns SELECT / EXCLUDE into one exact --includes
# per module. With neither, the whole client suite runs. Selectors are
# relative to <namespace>.tests.pychestrate.client or absolute.

SELECT      ?=
EXCLUDE     ?=
TEST_OUTPUT ?= .output/test-$(shell date +%Y%m%d-%H%M%S)

##@ Quality

test: ## Run the client tests: [SELECT="test_cli test_doctor"] [EXCLUDE=...] [TEST_OUTPUT=dir]
	@includes="$$($(PYTHON) development/select-tests --root $(TEST_ROOT) --suite $(TEST_SUITE) \
	    --base $(TEST_SUITE) --base $(TEST_NAMESPACE).tests.pychestrate --base $(TEST_NAMESPACE).tests \
	    $(foreach s,$(SELECT),--select $(s)) $(foreach x,$(EXCLUDE),--exclude $(x)))" || exit 2; \
	mkdir -p "$(TEST_OUTPUT)"; \
	echo "testbase: $$(echo "$$includes" | wc -l) include(s); output in $(TEST_OUTPUT)"; \
	PYTHONPATH="$(SOURCE_PYTHONPATH)$${PYTHONPATH:+:$$PYTHONPATH}" \
	    $(TESTBASE) testing run --root $(TEST_ROOT) $$includes --output="$(TEST_OUTPUT)" \
	    > "$(TEST_OUTPUT).log" 2>&1; \
	$(TESTBASE) agents summary extract --summary-file "$(TEST_OUTPUT)/testrun_summary.json"; \
	if ! grep -q '"result": "PASSED"' "$(TEST_OUTPUT)/testrun_summary.json" 2>/dev/null; then \
	    $(TESTBASE) agents testresult failures --results-file "$(TEST_OUTPUT)/testrun_results_stream.jsos" | head -80; \
	    echo "make test: FAILED (log: $(TEST_OUTPUT).log)" >&2; exit 1; \
	fi

lint: ## Static checks: Python syntax and testbase command traps
	$(PYTHON) development/lint

##@ Packaging

dist-clean: ## Remove dist/
	rm -fr dist

dist-build: ## Build the sdist and wheel into dist/
	$(POETRY) build

dist-publish: ## Upload dist/* to a PRIVATE index: REPOSITORY=<poetry repository name> CONFIRM=1
	@if [ -z "$(REPOSITORY)" ]; then \
		echo "dist-publish: refusing to run without REPOSITORY=<name>." >&2; \
		echo "  pychestrate-client is proprietary; a bare 'poetry publish' would upload it to public PyPI." >&2; \
		echo "  Configure the private index once:  poetry config repositories.<name> https://<private-index>/" >&2; \
		echo "  then:  make dist-publish REPOSITORY=<name> CONFIRM=1" >&2; \
		exit 2; \
	fi
	@case "$(REPOSITORY)" in \
		pypi|PyPI|testpypi|test-pypi) \
			echo "dist-publish: refusing to publish to the public index '$(REPOSITORY)'." >&2; exit 2;; \
	esac
	@url="$$($(POETRY) config repositories.$(REPOSITORY).url 2>/dev/null)"; status=$$?; \
	if [ $$status -ne 0 ] || [ -z "$$url" ] || [[ "$$url" != http* ]]; then \
		echo "dist-publish: poetry has no repository named '$(REPOSITORY)'." >&2; \
		echo "  poetry config repositories.$(REPOSITORY) https://<private-index>/" >&2; \
		exit 2; \
	fi; \
	case "$$url" in \
		*pypi.org*|*pythonhosted.org*) \
			echo "dist-publish: repository '$(REPOSITORY)' points at public PyPI ($$url); refusing." >&2; exit 2;; \
	esac; \
	if [ "$(CONFIRM)" != "1" ]; then \
		echo "dist-publish: would upload dist/* to '$(REPOSITORY)' ($$url)." >&2; \
		echo "  Re-run with CONFIRM=1 to upload." >&2; \
		exit 2; \
	fi; \
	$(POETRY) publish --repository "$(REPOSITORY)"
