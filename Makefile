PYTHON3_11_AVAILABLE := $(shell command -v python3.11 >/dev/null 2>&1 && echo yes)
PYTHON ?= $(if $(PYTHON3_11_AVAILABLE),python3.11,python3)
DEFAULT_VENV := $(if $(PYTHON3_11_AVAILABLE),.venv311,.venv)
ifeq ($(origin VENV), undefined)
VENV := $(DEFAULT_VENV)
endif
PYTHON_BIN = $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,$(PYTHON))
PIP_BIN = $(VENV)/bin/pip

DATASET ?= phase1
SEED ?= 42

RAW_DIR = data/raw/$(DATASET)/$(SEED)
STAGE_DIR = data/staging/$(DATASET)/$(SEED)
SCORE_JSON = reports/latest/score.json

SAMPLE_FORCE ?= 1
SAMPLE_FORCE_FLAG = $(if $(filter 1,$(SAMPLE_FORCE)),--force,)

INGEST_FORCE ?= 1
INGEST_FORCE_FLAG = $(if $(filter 1,$(INGEST_FORCE)),--force,)

RUN_ID_FROM_STAGE = $(PYTHON_BIN) scripts/get_run_id.py --stage-path "$(STAGE_DIR)"

.PHONY: setup sample ingest profile validate validate-only report run ensure-cmake

setup: ensure-cmake
	$(PYTHON) -m venv $(VENV)
	$(PIP_BIN) install --upgrade pip setuptools wheel
	PATH=$(VENV)/bin:$$PATH \
	PYARROW_BUNDLE_ARROW_CPP=1 \
	$(PIP_BIN) install -r requirements.txt

ensure-cmake:
	@if command -v cmake >/dev/null 2>&1; then \
		echo "cmake already installed at $$(command -v cmake)"; \
	elif command -v brew >/dev/null 2>&1; then \
		echo "cmake missing; installing via Homebrew..."; \
		brew install cmake; \
	else \
		echo "cmake is required to build pyarrow. Install it (e.g. 'brew install cmake' on macOS) and rerun 'make setup'."; \
		exit 1; \
	fi

sample:
	$(PYTHON_BIN) tools/generate_synthetic.py --dataset-name $(DATASET) --seed $(SEED) $(SAMPLE_FORCE_FLAG)

ingest:
	$(PYTHON_BIN) scripts/ingest.py --dataset-name $(DATASET) --seed $(SEED) $(INGEST_FORCE_FLAG)

profile:
	@if [ ! -d "$(STAGE_DIR)" ]; then \
		echo "Stage path $(STAGE_DIR) missing. Run `make ingest` first."; exit 1; \
	fi
	$(PYTHON_BIN) scripts/profile_tables.py --dataset-name $(DATASET) --seed $(SEED) --stage-path "$(STAGE_DIR)"

validate: ingest validate-only

validate-only:
	$(PYTHON_BIN) scripts/validate_runner.py --dataset-name $(DATASET) --seed $(SEED)
	@RUN_ID=$$($(RUN_ID_FROM_STAGE)); \
	$(PYTHON_BIN) scripts/score.py --run-id $$RUN_ID

report:
	@if [ ! -f "$(SCORE_JSON)" ]; then \
		echo "Score payload not found at $(SCORE_JSON). Run `make validate` first."; exit 1; \
	fi
	@RUN_ID=$$($(RUN_ID_FROM_STAGE)); \
	$(PYTHON_BIN) scripts/publish.py --run-id $$RUN_ID

run: sample ingest profile validate-only report
