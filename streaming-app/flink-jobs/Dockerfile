# Multi-stage build for Flink job fat JARs
FROM gradle:8.10-jdk17 AS builder

WORKDIR /build
COPY . .
RUN gradle shadowJar --no-daemon

# Runtime: official Flink image (multi-arch: amd64 + arm64)
FROM flink:1.19-java17

# Copy e-commerce job JARs
COPY --from=builder /build/enrichment/build/libs/enrichment-*-all.jar /opt/flink/usrlib/enrichment.jar
COPY --from=builder /build/aggregation/build/libs/aggregation-*-all.jar /opt/flink/usrlib/aggregation.jar
COPY --from=builder /build/fraud/build/libs/fraud-*-all.jar /opt/flink/usrlib/fraud.jar

# Copy IoT job JARs (IoT workload of the cross-workload campaign)
COPY --from=builder /build/iot-enrichment/build/libs/iot-enrichment-*-all.jar /opt/flink/usrlib/iot-enrichment.jar
COPY --from=builder /build/iot-aggregation/build/libs/iot-aggregation-*-all.jar /opt/flink/usrlib/iot-aggregation.jar
COPY --from=builder /build/iot-alert/build/libs/iot-alert-*-all.jar /opt/flink/usrlib/iot-alert.jar
