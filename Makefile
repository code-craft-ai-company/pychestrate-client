# Pychestrate pod client -- packaging entry points.
#
# pychestrate-client is proprietary: dist-publish uploads only to a
# private index that poetry knows by name, and only with CONFIRM=1.
# (pyproject.toml also carries the 'Private :: Do Not Upload'
# classifier, which PyPI rejects.)

SHELL := /bin/bash
POETRY ?= poetry

.PHONY: dist-clean dist-build dist-publish

dist-clean:
	rm -fr dist

dist-build:
	$(POETRY) build

dist-publish:
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
