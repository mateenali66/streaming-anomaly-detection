# syntax=docker/dockerfile:1.7
# Orchestrator image: kubectl 1.35 (checksum-verified), pinned Python 3.12 and a hash-locked set of libraries. NOTHING installs at
# run time (R1 ran apt-get, curl and pip when the Job started).
#
# Build (scripts/build_images.sh does this):
#   docker buildx build --platform linux/arm64 --provenance=false --sbom=false \
#     --build-arg PYTHON_BASE=<python image@digest from images.lock.yaml build_bases.python> \
#     --build-context orchestrator_src=<dir that CONTAINS the python package `orchestrator/`> \
#     -f images/orchestrator/Dockerfile images/orchestrator
#
# The orchestrator code is written by another builder. It is expected as a Python package `orchestrator` with a __main__.py;
# see README.md "Orchestrator image interface" for the entry point, environment and paths.
ARG PYTHON_BASE

FROM ${PYTHON_BASE} AS kubectl
# kubectl may differ from the API server by one minor version. EKS 1.35 => kubectl 1.35.x.
ARG KUBECTL_VERSION=v1.35.9
# sha256 of https://dl.k8s.io/release/v1.35.9/bin/linux/arm64/kubectl, recorded 13 Oct 2026. The build ALSO fetches
# kubectl.sha256 from dl.k8s.io and both must match, so a changed file at dl.k8s.io fails the build.
ARG KUBECTL_SHA256=39c98bca82875d9a9ddfb6f3c5ab17c3f78ea0801230683e538a67d5c053308d
RUN python - <<'PY'
import hashlib, os, sys, urllib.request
v, want = os.environ["KUBECTL_VERSION"], os.environ["KUBECTL_SHA256"]
base = f"https://dl.k8s.io/release/{v}/bin/linux/arm64/kubectl"
data = urllib.request.urlopen(base, timeout=120).read()
published = urllib.request.urlopen(base + ".sha256", timeout=60).read().decode().split()[0]
got = hashlib.sha256(data).hexdigest()
if not (got == want == published):
    sys.exit(f"kubectl checksum mismatch: downloaded {got}, pinned {want}, published {published}")
open("/kubectl", "wb").write(data)
os.chmod("/kubectl", 0o755)
print("kubectl", v, "sha256", got)
PY

FROM ${PYTHON_BASE}
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN groupadd --gid 10001 orchestrator \
 && useradd --uid 10001 --gid 10001 --home-dir /home/orchestrator --create-home --shell /usr/sbin/nologin orchestrator
COPY --from=kubectl /kubectl /usr/local/bin/kubectl
COPY requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: -r /tmp/requirements.lock \
 && rm /tmp/requirements.lock \
 && mkdir -p /opt/orchestrator /manifest /config \
 && chown 10001:10001 /manifest
# The orchestrator package (named build context). The build fails here if the context is missing.
COPY --from=orchestrator_src . /opt/orchestrator/
ENV PYTHONPATH=/opt/orchestrator HOME=/tmp KUBECTL_BIN=/usr/local/bin/kubectl
# Build-time proof that everything the code needs is present, and a record of what the image holds.
RUN python -c "import importlib.util as u, sys; sys.exit(0 if u.find_spec('orchestrator') else 'package orchestrator is missing from orchestrator_src')" \
 && python -c "import boto3, botocore, pyarrow, numpy, pandas, confluent_kafka, requests, yaml; assert confluent_kafka.version()[0] == '2.6.1', confluent_kafka.version(); print('boto3', boto3.__version__, 'pyarrow', pyarrow.__version__, 'numpy', numpy.__version__, 'pandas', pandas.__version__)" \
 && kubectl version --client=true -o yaml | grep -E 'gitVersion|platform' \
 && python -m pip freeze > /opt/pip-freeze.txt
USER 10001:10001
WORKDIR /tmp
ENTRYPOINT ["python", "-u", "-m", "orchestrator"]
CMD ["run"]
