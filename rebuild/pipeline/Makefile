# Preprocessing for the rebuilt benchmark. Run from this folder.
#   make build CAMPAIGN=/path/to/campaign/iot-b OUT=/path/to/out FREEZE=/path/to/FREEZE.json
# CAMPAIGN is a local mirror of campaign/<campaign_id>/. Outputs go to $(OUT)/derived/<campaign_id>/.

PY ?= python3
CAMPAIGN ?=
OUT ?=
FREEZE ?=
CONFIG ?=
CHECK ?=

ARGS = --campaign-dir $(CAMPAIGN) --out $(OUT) \
	$(if $(FREEZE),--freeze-marker $(FREEZE)) $(if $(CONFIG),--config $(CONFIG)) \
	$(if $(CHECK),--check-against $(CHECK))

.PHONY: build labels features folds validate synth test

build:
	$(PY) -m streamprep build $(ARGS) --stage all

labels:
	$(PY) -m streamprep build $(ARGS) --stage labels

features:
	$(PY) -m streamprep build $(ARGS) --stage features

folds:
	$(PY) -m streamprep build $(ARGS) --stage folds

validate:
	$(PY) -m streamprep validate --campaign-dir $(CAMPAIGN) $(if $(CONFIG),--config $(CONFIG))

synth:
	$(PY) -m streamprep synth --out $(OUT)/campaign --truth $(OUT)/truth

test:
	$(PY) -m pytest -q
