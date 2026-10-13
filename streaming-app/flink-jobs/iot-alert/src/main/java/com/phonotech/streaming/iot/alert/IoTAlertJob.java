package com.phonotech.streaming.iot.alert;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink Job 3 (IoT): Threshold-breach alerting.
 *
 * Stateless filter+transform: emits an alert when an enriched IoT event's
 * temperature exceeds 40 C OR battery_level falls below 10 percent. Alerts
 * are written to fraud-alerts, the topic the e-commerce fraud job uses, so the
 * same Kafka topics manifest and monitoring dashboards serve both workloads.
 *
 * Input:  enriched-events (JSON)
 * Output: fraud-alerts (JSON, one per breached event)
 */
public class IoTAlertJob {

    private static final double TEMPERATURE_THRESHOLD = 40.0;
    private static final double BATTERY_THRESHOLD = 10.0;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = getParam(args, "bootstrap.servers",
                "streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092");
        String inputTopic = getParam(args, "input.topic", "enriched-events");
        String outputTopic = getParam(args, "output.topic", "fraud-alerts");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("iot-alert-job")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> enriched = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "iot-enriched-events-source");

        DataStream<String> alerts = enriched
                .flatMap(new ThresholdBreachFlatMap())
                .name("iot-threshold-breach");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
        alerts.sinkTo(sink).name("iot-alerts-sink");

        env.execute("IoT Threshold Alert Job");
    }

    public static class ThresholdBreachFlatMap implements FlatMapFunction<String, String> {
        @Override
        public void flatMap(String value, Collector<String> out) {
            try {
                Double temperature = extractDouble(value, "temperature");
                Double battery = extractDouble(value, "battery_level");
                String deviceId = extractStringField(value, "device_id");
                Integer locationId = extractIntField(value, "location_id");
                String region = extractStringField(value, "region");

                boolean tempBreach = temperature != null && temperature > TEMPERATURE_THRESHOLD;
                boolean batteryBreach = battery != null && battery < BATTERY_THRESHOLD;
                if (!tempBreach && !batteryBreach) return;

                StringBuilder reason = new StringBuilder();
                if (tempBreach) reason.append("temperature_high");
                if (tempBreach && batteryBreach) reason.append("+");
                if (batteryBreach) reason.append("battery_low");

                String alert = String.format(
                        "{\"alert_type\":\"iot_threshold_breach\","
                                + "\"device_id\":\"%s\","
                                + "\"location_id\":%s,"
                                + "\"region\":\"%s\","
                                + "\"temperature\":%.3f,"
                                + "\"battery_level\":%.3f,"
                                + "\"reason\":\"%s\","
                                + "\"timestamp\":%d}",
                        deviceId == null ? "unknown" : deviceId,
                        locationId == null ? "null" : locationId.toString(),
                        region == null ? "unknown" : region,
                        temperature == null ? Double.NaN : temperature,
                        battery == null ? Double.NaN : battery,
                        reason.toString(),
                        System.currentTimeMillis()
                );
                out.collect(alert);
            } catch (Exception e) {
                // swallow malformed records
            }
        }

        private Double extractDouble(String json, String field) {
            String pattern = "\"" + field + "\":";
            int start = json.indexOf(pattern);
            if (start < 0) return null;
            start += pattern.length();
            int end = start;
            while (end < json.length()
                    && (Character.isDigit(json.charAt(end))
                        || json.charAt(end) == '.'
                        || json.charAt(end) == '-'
                        || json.charAt(end) == 'e'
                        || json.charAt(end) == 'E'
                        || json.charAt(end) == '+')) {
                end++;
            }
            if (end == start) return null;
            try {
                return Double.parseDouble(json.substring(start, end));
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private Integer extractIntField(String json, String field) {
            Double v = extractDouble(json, field);
            return v == null ? null : v.intValue();
        }

        private String extractStringField(String json, String field) {
            String pattern = "\"" + field + "\":\"";
            int start = json.indexOf(pattern);
            if (start < 0) return null;
            start += pattern.length();
            int end = json.indexOf('"', start);
            if (end < 0) return null;
            return json.substring(start, end);
        }
    }

    private static String getParam(String[] args, String key, String defaultValue) {
        for (String arg : args) {
            if (arg.startsWith("--" + key + "=")) {
                return arg.substring(("--" + key + "=").length());
            }
        }
        String envKey = key.replace(".", "_").toUpperCase();
        return System.getenv(envKey) != null ? System.getenv(envKey) : defaultValue;
    }
}
