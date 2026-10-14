# syntax=docker/dockerfile:1.7
# Flink session image: the six job fat JARs, the S3 filesystem plugin (checkpoints and savepoints on S3), submit-jobs.sh and
# run-job.sh (the one submission path, with the job settings: REVIEW4_LIVE_CONTRACT L1).
#   docker buildx build --platform linux/arm64 --provenance=false --sbom=false \
#     --build-arg GRADLE_BASE=<gradle image@digest> --build-arg FLINK_BASE=<flink image@digest> \
#     --build-context flink_src=streaming-app/flink-jobs \
#     -f rebuild/infra/images/flink-jobs/Dockerfile rebuild/infra/images/flink-jobs
# Both bases come from images.lock.yaml `build_bases` (digest-pinned). ONE Flink version, 1.19.3, is the runtime; build.gradle
# compiles against 1.19.1 (same minor, same API), unchanged, because streaming-app/ is not modified here.
ARG GRADLE_BASE
ARG FLINK_BASE

FROM ${GRADLE_BASE} AS builder
WORKDIR /build
COPY --from=flink_src . .
# The host tree may carry old build output and Gradle caches. Start clean.
RUN rm -rf .gradle build && for d in */; do rm -rf "${d}build"; done
COPY lock-init.gradle /build-support/lock-init.gradle
COPY locks/ /build-support/locks/
# STRICT dependency locking: a changed dependency version fails the build instead of changing the jars.
RUN gradle --no-daemon --console=plain -I /build-support/lock-init.gradle -Dlocks.dir=/build-support/locks shadowJar \
 && ls -l */build/libs/*-all.jar

FROM ${FLINK_BASE}
USER root
# S3 filesystem plugin, the way the Flink 1.19 docs describe it: copy the jar from opt/ into plugins/<name>/.
# flink-s3-fs-hadoop 1.19.3 bundles AWS SDK for Java 1.12.779 (>= 1.12.746, the Pod Identity minimum).
RUN set -eux; ls /opt/flink/opt/flink-s3-fs-hadoop-*.jar; \
    mkdir -p /opt/flink/plugins/s3-fs-hadoop; \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/; \
    mkdir -p /opt/flink/usrlib
COPY --from=builder /build/enrichment/build/libs/enrichment-1.0.0-all.jar        /opt/flink/usrlib/enrichment.jar
COPY --from=builder /build/aggregation/build/libs/aggregation-1.0.0-all.jar      /opt/flink/usrlib/aggregation.jar
COPY --from=builder /build/fraud/build/libs/fraud-1.0.0-all.jar                  /opt/flink/usrlib/fraud.jar
COPY --from=builder /build/iot-enrichment/build/libs/iot-enrichment-1.0.0-all.jar   /opt/flink/usrlib/iot-enrichment.jar
COPY --from=builder /build/iot-aggregation/build/libs/iot-aggregation-1.0.0-all.jar /opt/flink/usrlib/iot-aggregation.jar
COPY --from=builder /build/iot-alert/build/libs/iot-alert-1.0.0-all.jar          /opt/flink/usrlib/iot-alert.jar
COPY --chmod=0755 submit-jobs.sh /opt/flink/bin/submit-jobs.sh
COPY --chmod=0755 run-job.sh /opt/flink/bin/run-job.sh
RUN sha256sum /opt/flink/usrlib/*.jar > /opt/flink/usrlib/SHA256SUMS && chown -R flink:flink /opt/flink/usrlib /opt/flink/plugins
USER flink
