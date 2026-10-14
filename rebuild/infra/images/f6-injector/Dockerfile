# syntax=docker/dockerfile:1.7
# Prebuilt F6 poison-pill injector. R1 piped a script through a ConfigMap and ran `pip install confluent-kafka` (unpinned) at
# start; the ConfigMap could not even be created (no RBAC). Now it is an image: nothing installs at run time.
#
# The injector code is the ONE implementation in the orchestrator package, rebuild/orchestrator/orchestrator/f6_injector.py
# (rebuild/CONTRACT_CHANGES.md, item C6). It tags every record with the header rebuild-attempt=<ATTEMPT_KEY>, counts the
# deliveries the brokers acknowledged and logs the first and last acknowledgement times. The file imports only the standard
# library at module level, so it runs here as a plain script; nothing else of the orchestrator package is copied.
#   docker buildx build --platform linux/arm64 --provenance=false --sbom=false --build-arg PYTHON_BASE=<image@digest> \
#     --build-context orchestrator_src=rebuild/orchestrator -f images/f6-injector/Dockerfile images/f6-injector
# (scripts/build_images.sh passes the same build context as for the orchestrator image.)
ARG PYTHON_BASE
FROM ${PYTHON_BASE}
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN groupadd --gid 10001 injector && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin injector
COPY requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --require-hashes --only-binary=:all: -r /tmp/requirements.lock \
 && rm /tmp/requirements.lock \
 && python -c "import confluent_kafka; assert confluent_kafka.version()[0] == '2.6.1'"
# The build fails here if the named context is missing or the file moved.
COPY --from=orchestrator_src orchestrator/f6_injector.py /opt/injector/f6_injector.py
# Build-time proof: header on every record, exact planned count, no closing brace, acknowledgement times, parseable result.
RUN python /opt/injector/f6_injector.py --self-test && sha256sum /opt/injector/f6_injector.py > /opt/injector/SOURCE.sha256
USER 10001:10001
ENTRYPOINT ["python", "-u", "/opt/injector/f6_injector.py"]
