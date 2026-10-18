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

.PHONY: build labels features folds validate synth test baselines metrics stats

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

# Analysis (protocol 7.1 steps 7 and 8), added after registration and recorded as D4 in protocol/DEVIATIONS.md.
#   make baselines DERIVED=/path/derived BASELINES=/path/baselines FREEZE=../frozen/FREEZE.json
#   make metrics stats DERIVED=... PHASE_B=/path/fz-run BASELINES=... RESULTS=/path/results FREEZE=... APY=/path/python
# DERIVED holds derived/<campaign_id>/ for all six campaigns. baselines imports training/train.py and so runs in the
# training environment. metrics and stats run in the environment of analysis/requirements.lock (APY).
APY ?= $(PY)
DERIVED ?=
PHASE_B ?=
BASELINES ?=
RESULTS ?=
CAMPAIGNS ?= ec-a ec-b ec-c iot-a iot-b iot-c

baselines:
	for c in $(CAMPAIGNS); do $(PY) analysis/baselines.py --derived $(DERIVED) --campaign $$c --out $(BASELINES) \
		$(if $(FREEZE),--freeze-marker $(FREEZE)) || exit 1; done

metrics:
	$(APY) -m analysis.run metrics --derived $(DERIVED) --phase-b $(PHASE_B) --baselines $(BASELINES) --out $(RESULTS) \
		$(if $(FREEZE),--freeze-marker $(FREEZE))

stats:
	$(APY) -m analysis.run stats --out $(RESULTS)
