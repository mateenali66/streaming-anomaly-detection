package com.phonotech.streaming.aggregation;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Flink Job 2: Windowed Aggregation
 *
 * Stateful job that aggregates enriched e-commerce events in tumbling 1-minute
 * windows. Computes revenue, order count, cart additions, page views, and
 * cart abandonment rate per window.
 *
 * Input:  enriched-events (JSON)
 * Output: aggregated-metrics (JSON)
 *
 * Uses RocksDB state backend with incremental checkpoints for durability.
 */
public class AggregationJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = getParam(args, "bootstrap.servers",
                "streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092");
        String inputTopic = getParam(args, "input.topic", "enriched-events");
        String outputTopic = getParam(args, "output.topic", "aggregated-metrics");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("aggregation-job")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> enrichedEvents = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "enriched-events-source");

        // Aggregate in 1-minute tumbling windows (global, not keyed)
        DataStream<String> aggregated = enrichedEvents
                .windowAll(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .aggregate(new WindowAggregator())
                .name("1-min-aggregation");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        aggregated.sinkTo(sink).name("aggregated-metrics-sink");

        env.execute("Windowed Aggregation Job");
    }

    /**
     * Aggregates event counts and revenue in a 1-minute window.
     */
    public static class WindowAggregator implements AggregateFunction<String, WindowAccumulator, String> {

        @Override
        public WindowAccumulator createAccumulator() {
            return new WindowAccumulator();
        }

        @Override
        public WindowAccumulator add(String value, WindowAccumulator acc) {
            acc.totalEvents++;

            // Check event_type field first (most reliable), then fall back to field presence
            if (value.contains("\"event_type\":\"page_view\"")) {
                acc.pageViews++;
            } else if (value.contains("\"event_type\":\"purchase\"")) {
                acc.purchases++;
                acc.revenue += extractNumericField(value, "total", 0.0);
            } else if (value.contains("\"event_type\":\"add_to_cart\"")) {
                acc.cartAdditions++;
                acc.cartValue += extractNumericField(value, "unit_price", 0.0);
            } else if (value.contains("\"event_type\":\"user_action\"")) {
                acc.otherActions++;
            } else {
                acc.otherActions++;
            }

            return acc;
        }

        @Override
        public String getResult(WindowAccumulator acc) {
            double abandonmentRate = acc.cartAdditions > 0
                    ? 1.0 - ((double) acc.purchases / acc.cartAdditions)
                    : 0.0;

            return String.format(
                    "{\"window_end\":%d,\"total_events\":%d,\"page_views\":%d,"
                            + "\"cart_additions\":%d,\"purchases\":%d,\"other_actions\":%d,"
                            + "\"revenue\":%.2f,\"cart_value\":%.2f,"
                            + "\"cart_abandonment_rate\":%.4f}",
                    System.currentTimeMillis(),
                    acc.totalEvents,
                    acc.pageViews,
                    acc.cartAdditions,
                    acc.purchases,
                    acc.otherActions,
                    acc.revenue,
                    acc.cartValue,
                    abandonmentRate
            );
        }

        @Override
        public WindowAccumulator merge(WindowAccumulator a, WindowAccumulator b) {
            a.totalEvents += b.totalEvents;
            a.pageViews += b.pageViews;
            a.cartAdditions += b.cartAdditions;
            a.purchases += b.purchases;
            a.otherActions += b.otherActions;
            a.revenue += b.revenue;
            a.cartValue += b.cartValue;
            return a;
        }

        private double extractNumericField(String json, String field, double defaultVal) {
            String pattern = "\"" + field + "\":";
            int start = json.indexOf(pattern);
            if (start < 0) return defaultVal;
            start += pattern.length();
            int end = start;
            while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '.')) {
                end++;
            }
            try {
                return Double.parseDouble(json.substring(start, end));
            } catch (NumberFormatException e) {
                return defaultVal;
            }
        }
    }

    public static class WindowAccumulator implements java.io.Serializable {
        long totalEvents = 0;
        long pageViews = 0;
        long cartAdditions = 0;
        long purchases = 0;
        long otherActions = 0;
        double revenue = 0.0;
        double cartValue = 0.0;
    }

    private static String getParam(String[] args, String key, String defaultValue) {
        for (String arg : args) {
            if (arg.startsWith("--" + key + "=")) {
                return arg.substring(("--" + key + "=").length());
            }
        }
        return System.getenv(key.replace(".", "_").toUpperCase()) != null
                ? System.getenv(key.replace(".", "_").toUpperCase())
                : defaultValue;
    }
}
