# syntax=docker/dockerfile:1.7
# Load generator: the unmodified streaming-app generator plus loadgen_entry.py (seed, Prometheus metrics, restart-safe phase).
#   docker buildx build --platform linux/arm64 --provenance=false --sbom=false --build-arg PYTHON_BASE=<image@digest> \
#     --build-context app=streaming-app \
#     -f rebuild/infra/images/load-generator/Dockerfile rebuild/infra/images/load-generator
ARG PYTHON_BASE
FROM ${PYTHON_BASE}
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN groupadd --gid 10001 loadgen && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin loadgen
COPY requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: -r /tmp/requirements.lock \
 && rm /tmp/requirements.lock
WORKDIR /app/load-generator
COPY --from=app load-generator/generator.py load-generator/config.py ./
COPY --from=app schemas/ /app/schemas/
COPY loadgen_entry.py ./
# Record exactly which generator source was baked in.
RUN sha256sum generator.py config.py loadgen_entry.py > /app/SOURCE.sha256 \
 && python -c "import ast,sys; [ast.parse(open(f).read()) for f in ('generator.py','config.py','loadgen_entry.py')]" \
 && python -c "import confluent_kafka, prometheus_client; assert confluent_kafka.version()[0] == '2.6.1'"
USER 10001:10001
ENTRYPOINT ["python", "-u", "loadgen_entry.py"]
