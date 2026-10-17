# syntax=docker/dockerfile:1.7
# Training image (protocol 7.3.1): pinned CPython 3.14.8 and the hash-locked training environment, nothing else. Added on
# 17 October 2026, after registration.
#
# The code is not baked in. A `git archive` of one commit is mounted read-only at /code, so the same image serves every
# training phase, and a run is pinned by the image digest together with the commit. The CUDA user-space libraries come from
# the locked nvidia-* wheels (torch 2.12.1 on PyPI is the CUDA 13.0 build). The driver comes from the host through the NVIDIA
# container toolkit (`docker run --gpus`). Nothing installs packages at run time.
#
# Build on the x86_64 training machine (run_in_image.sh records the image ID and the versions):
#   docker buildx build --platform linux/amd64 --provenance=false --sbom=false --load -t sad-training:r2-001 \
#     pipeline/training/image
#
# python:3.14.8-slim-trixie, linux/amd64 manifest, resolved on 17 Oct 2026 (index sha256:a2b82f3c48559aa0a8446d9af49826b6
# e2b2016f4cd2afabfe6013ec53729170).
FROM python:3.14.8-slim-trixie@sha256:cfe2e24a75302a15934d37c2d86412893c0aa934dc3a97cbb439d04c01890ca9

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
RUN groupadd --gid 10001 trainer \
 && useradd --uid 10001 --gid 10001 --home-dir /home/trainer --create-home --shell /usr/sbin/nologin trainer
COPY requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: --no-deps -r /tmp/requirements.lock \
 && pip check \
 && rm /tmp/requirements.lock

# Versions recorded in the image itself. The CUDA and cuDNN versions are those torch was built with and loads. No GPU is
# needed to read them.
RUN python - <<'PY' > /etc/training-env.txt && cat /etc/training-env.txt
import platform, importlib.metadata as md
import torch
print("python", platform.python_version(), platform.python_implementation())
print("torch", torch.__version__, "cuda", torch.version.cuda, "cudnn", torch.backends.cudnn.version())
for name in ["numpy", "pandas", "pyarrow", "scipy", "scikit-learn", "optuna", "SQLAlchemy", "alembic", "joblib",
             "threadpoolctl", "colorlog", "tqdm", "PyYAML", "pytest"]:
    print(name, md.version(name))
PY

USER 10001:10001
ENV HOME=/home/trainer
WORKDIR /home/trainer
