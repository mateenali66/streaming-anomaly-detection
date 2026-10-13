package com.phonotech.streaming.fraud;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Flink Job 3: Fraud Detection
 *
 * Pattern matching job that detects potential fraud:
 * - More than 5 purchase events from the same IP in a 10-second sliding window
 * - Total order value exceeding $10,000 in the window
 *
 * Input:  enriched-events (JSON)
 * Output: fraud-alerts (JSON)
 */
public class FraudDetectionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = getParam(args, "bootstrap.servers",
                "streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092");
        String inputTopic = getParam(args, "input.topic", "enriched-events");
        String outputTopic = getParam(args, "output.topic", "fraud-alerts");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("fraud-detection-job")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> enrichedEvents = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "enriched-events-source");

        // Filter to purchase events only, key by IP address
        DataStream<String> purchaseEvents = enrichedEvents
                .filter(event -> event.contains("\"order_id\""))
                .name("purchase-filter");

        // Sliding window: 10s window, 5s slide, keyed by IP
        DataStream<String> fraudAlerts = purchaseEvents
                .keyBy(new IpKeySelector())
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new FraudAggregator(), new FraudAlertEmitter())
                .name("fraud-pattern-detection");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(outputTopic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();

        fraudAlerts.sinkTo(sink).name("fraud-alerts-sink");

        env.execute("Fraud Detection Job");
    }

    /**
     * Extracts IP address from JSON event for keying.
     */
    public static class IpKeySelector implements KeySelector<String, String> {
        @Override
        public String getKey(String value) {
            String pattern = "\"ip_address\":\"";
            int start = value.indexOf(pattern);
            if (start < 0) return "unknown";
            start += pattern.length();
            int end = value.indexOf('"', start);
            if (end < 0) return "unknown";
            return value.substring(start, end);
        }
    }

    /**
     * Accumulates purchase count and total value per window.
     */
    public static class FraudAggregator implements AggregateFunction<String, FraudAccumulator, FraudAccumulator> {

        @Override
        public FraudAccumulator createAccumulator() {
            return new FraudAccumulator();
        }

        @Override
        public FraudAccumulator add(String value, FraudAccumulator acc) {
            acc.purchaseCount++;
            acc.totalValue += extractTotal(value);
            return acc;
        }

        @Override
        public FraudAccumulator getResult(FraudAccumulator acc) {
            return acc;
        }

        @Override
        public FraudAccumulator merge(FraudAccumulator a, FraudAccumulator b) {
            a.purchaseCount += b.purchaseCount;
            a.totalValue += b.totalValue;
            return a;
        }

        private double extractTotal(String json) {
            String pattern = "\"base_price\":";
            int start = json.indexOf(pattern);
            if (start < 0) return 0.0;
            start += pattern.length();
            int end = start;
            while (end < json.length() && (Character.isDigit(json.charAt(end)) || json.charAt(end) == '.')) {
                end++;
            }
            try {
                return Double.parseDouble(json.substring(start, end));
            } catch (NumberFormatException e) {
                return 0.0;
            }
        }
    }

    /**
     * Emits fraud alert only if thresholds are exceeded.
     */
    public static class FraudAlertEmitter extends ProcessWindowFunction<FraudAccumulator, String, String, TimeWindow> {

        private static final int ORDER_COUNT_THRESHOLD = 5;
        private static final double VALUE_THRESHOLD = 10000.0;

        @Override
        public void process(String ipAddress, Context context, Iterable<FraudAccumulator> elements, Collector<String> out) {
            FraudAccumulator acc = elements.iterator().next();

            boolean countExceeded = acc.purchaseCount > ORDER_COUNT_THRESHOLD;
            boolean valueExceeded = acc.totalValue > VALUE_THRESHOLD;

            if (countExceeded || valueExceeded) {
                String alert = String.format(
                        "{\"alert_type\":\"potential_fraud\","
                                + "\"ip_address\":\"%s\","
                                + "\"window_start\":%d,"
                                + "\"window_end\":%d,"
                                + "\"purchase_count\":%d,"
                                + "\"total_value\":%.2f,"
                                + "\"reason\":\"%s\","
                                + "\"timestamp\":%d}",
                        ipAddress,
                        context.window().getStart(),
                        context.window().getEnd(),
                        acc.purchaseCount,
                        acc.totalValue,
                        (countExceeded ? "high_order_count" : "") + (countExceeded && valueExceeded ? "+" : "") + (valueExceeded ? "high_value" : ""),
                        System.currentTimeMillis()
                );
                out.collect(alert);
            }
        }
    }

    public static class FraudAccumulator implements java.io.Serializable {
        int purchaseCount = 0;
        double totalValue = 0.0;
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
