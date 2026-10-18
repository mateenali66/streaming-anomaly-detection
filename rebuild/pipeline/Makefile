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

.PHONY: build labels features folds validate synth test baselines metrics stats posthoc describe numbers tables figures check from-scores verify-inputs gate ewma-mechanism classical ewma-r1-inputs posthoc-review build-c1 build-replicates train freeze train-frozen all explore cost facts paper-sync

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

# D5 post hoc analyses, descriptive tables, numbers.tex, tables, figures and the claims harness (7.1 steps 9 to 11).
#   make from-scores ... RESULTS=/path/results [MANUSCRIPT=main.tex ALLOW=allowed.json PDF=main.pdf BUILD_RECORD=build.json]
MANUSCRIPT ?=
ALLOW ?=
PDF ?=
BUILD_RECORD ?=

posthoc:
	$(APY) -m analysis.posthoc --out $(RESULTS)

describe:
	$(APY) -m analysis.describe --out $(RESULTS)

numbers:
	$(APY) -m analysis.numbers_tex --out $(RESULTS) --tex $(RESULTS)/numbers.tex --claims $(RESULTS)/claims.json

tables:
	$(APY) -m analysis.tables --out $(RESULTS) --dest $(RESULTS)/tables
	if [ -d $(RESULTS)/explore ]; then $(APY) -m analysis.tables_explore --out $(RESULTS) --dest $(RESULTS)/tables \
		--frozen ../frozen; fi

figures:
	$(APY) -m analysis.figures --out $(RESULTS) --dest $(RESULTS)/figures

check:
	$(APY) -m analysis.check --results $(RESULTS) --numbers $(RESULTS)/numbers.tex --claims $(RESULTS)/claims.json \
		$(if $(MANUSCRIPT),--tex $(MANUSCRIPT)) $(if $(ALLOW),--allow $(ALLOW)) $(if $(PDF),--pdf $(PDF)) \
		$(if $(BUILD_RECORD),--build-record $(BUILD_RECORD))

from-scores: metrics stats posthoc describe numbers tables figures check

# The remaining steps of protocol 7.1, added after registration (D6). COPIES holds each campaign's verified copy as
# <cid><COPY_SUFFIX>/ (the deposit's campaign/<cid>/ with COPY_SUFFIX empty). EXTRA holds the exploratory scores.
# CPY is an environment with pyod and stumpy for the classical detectors (5.9.6).
COPIES ?=
COPY_SUFFIX ?= -final
EXTRA ?=
CPY ?= $(PY)
IMAGE ?=
CODE ?=
TRAIN_RUN ?=
FROZEN_RUN ?=
SHARED ?=

verify-inputs:
	for c in $(CAMPAIGNS); do $(PY) -m streamprep validate --campaign-dir $(COPIES)/$$c$(COPY_SUFFIX) || exit 1; done

gate:
	$(APY) -m analysis.gate_summary --copies $(COPIES) --copy-suffix='$(COPY_SUFFIX)' --derived $(DERIVED) \
		--out $(RESULTS)/gate --offline-gate

ewma-mechanism:
	for c in $(CAMPAIGNS); do $(PY) -m analysis.ewma_mechanism score --derived $(DERIVED) --campaign $$c --out $(EXTRA) \
		$(if $(FREEZE),--freeze-marker $(FREEZE)) || exit 1; done
	$(APY) -m analysis.ewma_mechanism summarize --derived $(DERIVED) --phase-b $(PHASE_B) --baselines $(BASELINES) \
		--extra $(EXTRA) --results $(RESULTS) --out $(RESULTS)/ewma_mechanism

classical:
	for c in $(CAMPAIGNS); do \
		$(APY) -m analysis.classical rules --derived $(DERIVED) --campaign $$c --out $(EXTRA) $(if $(FREEZE),--freeze-marker $(FREEZE)) && \
		$(CPY) -m analysis.classical detectors --derived $(DERIVED) --campaign $$c --out $(EXTRA) $(if $(FREEZE),--freeze-marker $(FREEZE)) \
		|| exit 1; done
	$(APY) -m analysis.classical summarize --derived $(DERIVED) --extra $(EXTRA) --results $(RESULTS) --out $(RESULTS)/classical

# Sensitivity checks added after the internal review of 18 October (D6, item 10): EWMA on the earlier manuscript's
# inputs (training environment), then the post hoc checks, which read the stored scores and the explore outputs.
ewma-r1-inputs:
	for c in $(CAMPAIGNS); do $(PY) -m analysis.ewma_r1_inputs --derived $(DERIVED) --campaign $$c --out $(EXTRA) \
		$(if $(FREEZE),--freeze-marker $(FREEZE)) || exit 1; done

posthoc-review:
	$(APY) -m analysis.posthoc_review --derived $(DERIVED) --phase-b $(PHASE_B) --baselines $(BASELINES) --extra $(EXTRA) \
		--results $(RESULTS) --drift

# Training (7.1 step 6) runs in the training image on a GPU machine: the campaign-1 search and window jobs, then the
# freeze, then the frozen, category and cross-workload fits (6.3). make all = verify-inputs, the builds, these, and
# from-scores. The freeze writes ../frozen, which the author commits and tags before train-frozen reads campaigns 2
# and 3 (6.1.2, 7.1).
# BUILD_OUT/derived is DERIVED. Campaign 1 builds before training. The replicates build with the freeze marker,
# which only exists after the freeze (6.1, 7.1).
BUILD_OUT ?=
build-c1:
	for c in ec-a iot-a; do $(PY) -m streamprep build --campaign-dir $(COPIES)/$$c$(COPY_SUFFIX) --out $(BUILD_OUT) \
		--stage all || exit 1; done

build-replicates:
	for c in ec-b ec-c iot-b iot-c; do $(PY) -m streamprep build --campaign-dir $(COPIES)/$$c$(COPY_SUFFIX) \
		--out $(BUILD_OUT) --stage all --freeze-marker $(FREEZE) || exit 1; done
	$(PY) training/cross_train.py --write-shared $(DERIVED)/shared_30s.json --derived $(DERIVED)

train:
	training/image/run_in_image.sh run --image $(IMAGE) --code $(CODE) --derived $(DERIVED) --out $(TRAIN_RUN) \
		-- --phases search window

freeze:
	$(PY) training/freeze.py --results $(TRAIN_RUN) --out ../frozen --expect-trials 100

train-frozen:
	training/image/run_in_image.sh run --image $(IMAGE) --code $(CODE) --derived $(DERIVED) --out $(FROZEN_RUN) \
		-- --phases frozen category cross --frozen-dir /code/rebuild/frozen --shared /data/shared_30s.json

all: verify-inputs build-c1 train freeze build-replicates train-frozen baselines metrics stats posthoc describe gate ewma-mechanism classical ewma-r1-inputs explore posthoc-review cost facts numbers tables figures check

# Exploratory analyses of 5.9 from the stored scores (D6), and the cost measurement of 5.9.18 (a GPU-free fit per
# model on one machine, in the training environment).
explore:
	$(APY) -m analysis.explore --derived $(DERIVED) --phase-b $(PHASE_B) --baselines $(BASELINES) --search-run $(TRAIN_RUN) \
		--copies $(COPIES) --copy-suffix='$(COPY_SUFFIX)' --results $(RESULTS)

cost:
	$(PY) -m analysis.cost --derived $(DERIVED) --frozen-dir ../frozen --out $(RESULTS)/cost --phase-b $(PHASE_B)

# Facts the manuscript quotes (build sizes, training job counts, frozen settings), and the copy of numbers.tex, the
# tables and the figures into the manuscript folder (PAPER, for example ../../array/r2).
PAPER ?=
facts:
	$(APY) -m analysis.facts --derived $(DERIVED) --search-run $(TRAIN_RUN) --phase-b $(PHASE_B) --frozen ../frozen \
		--out $(RESULTS)

paper-sync:
	mkdir -p $(PAPER)/tables $(PAPER)/figures
	cp $(RESULTS)/numbers.tex $(RESULTS)/claims.json $(PAPER)/
	cp $(RESULTS)/tables/*.tex $(PAPER)/tables/
	cp $(RESULTS)/figures/*.pdf $(PAPER)/figures/
