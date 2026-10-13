package com.phonotech.streaming.iot.enrichment;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

/**
 * Flink Job 1 (IoT): Sensor-event Enrichment
 *
 * Reads raw IoT sensor events from raw-events, looks up the location_id in a
 * static metadata catalog (region/zone/site_type), and writes enriched events
 * to enriched-events. Mirrors the e-commerce enrichment job in pipeline shape
 * (Kafka source -> ProcessFunction -> Kafka sink with DLQ side output), so the
 * cross-workload comparison keeps the pipeline structure the same.
 *
 * Input:  raw-events (JSON)
 * Output: enriched-events (JSON with region, zone, site_type appended)
 * Error:  dead-letter (deserialization failures)
 */
public class IoTEnrichmentJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = getParam(args, "bootstrap.servers",
                "streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092");
        String groupId = getParam(args, "group.id", "iot-enrichment-job");
        String inputTopic = getParam(args, "input.topic", "raw-events");
        String outputTopic = getParam(args, "output.topic", "enriched-events");
        String dlqTopic = getParam(args, "dlq.topic", "dead-letter");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "iot-raw-events-source");

        OutputTag<String> dlqTag = new OutputTag<String>("dead-letter") {};

        SingleOutputStreamOperator<String> enriched = rawEvents
                .process(new IoTEnrichmentProcessFunction(dlqTag))
                .name("iot-event-enrichment");

        KafkaSink<String> enrichedSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
        enriched.sinkTo(enrichedSink).name("enriched-events-sink");

        DataStream<String> deadLetters = enriched.getSideOutput(dlqTag);
        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(dlqTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
        deadLetters.sinkTo(dlqSink).name("dead-letter-sink");

        env.execute("IoT Event Enrichment Job");
    }

    public static class IoTEnrichmentProcessFunction extends ProcessFunction<String, String> {
        private final OutputTag<String> dlqTag;
        private transient Map<Integer, LocationInfo> catalog;
        private static final int NUM_LOCATIONS = 500;
        private static final String[] REGIONS = {"us-east", "us-west", "eu-central", "ap-southeast", "sa-east"};
        private static final String[] ZONES = {"a", "b", "c", "d", "e"};
        private static final String[] SITE_TYPES = {"warehouse", "factory", "office", "retail", "datacenter"};

        public IoTEnrichmentProcessFunction(OutputTag<String> dlqTag) {
            this.dlqTag = dlqTag;
        }

        @Override
        public void open(Configuration parameters) {
            catalog = new HashMap<>();
            for (int i = 0; i < NUM_LOCATIONS; i++) {
                String region = REGIONS[i % REGIONS.length];
                String zone = ZONES[(i / REGIONS.length) % ZONES.length];
                String siteType = SITE_TYPES[i % SITE_TYPES.length];
                catalog.put(i, new LocationInfo(region, zone, siteType));
            }
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) {
            try {
                Integer locationId = extractIntField(value, "location_id");
                if (locationId != null && catalog.containsKey(locationId)) {
                    LocationInfo info = catalog.get(locationId);
                    String enriched = value.substring(0, value.lastIndexOf('}'))
                            + ",\"region\":\"" + info.region + "\""
                            + ",\"zone\":\"" + info.zone + "\""
                            + ",\"site_type\":\"" + info.siteType + "\""
                            + ",\"enriched\":true"
                            + "}";
                    out.collect(enriched);
                } else {
                    String passthrough = value.substring(0, value.lastIndexOf('}'))
                            + ",\"enriched\":false}";
                    out.collect(passthrough);
                }
            } catch (Exception e) {
                String dlqMessage = "{\"original\":" + escapeJson(value)
                        + ",\"error\":\"" + escapeJson(e.getMessage())
                        + "\",\"timestamp\":" + System.currentTimeMillis() + "}";
                ctx.output(dlqTag, dlqMessage);
            }
        }

        private Integer extractIntField(String json, String field) {
            String pattern = "\"" + field + "\":";
            int start = json.indexOf(pattern);
            if (start < 0) return null;
            start += pattern.length();
            int end = start;
            while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '-')) {
                end++;
            }
            if (end == start) return null;
            try {
                return Integer.parseInt(json.substring(start, end));
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private String escapeJson(String s) {
            if (s == null) return "null";
            return s.replace("\\", "\\\\").replace("\"", "\\\"");
        }

        private static class LocationInfo implements java.io.Serializable {
            final String region;
            final String zone;
            final String siteType;
            LocationInfo(String region, String zone, String siteType) {
                this.region = region;
                this.zone = zone;
                this.siteType = siteType;
            }
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
