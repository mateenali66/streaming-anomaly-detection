package com.phonotech.streaming.iot.aggregation;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Flink Job 2 (IoT): Per-location windowed aggregation.
 *
 * Stateful job keyed by location_id with 1-second tumbling windows. Computes
 * temperature mean / max / p99, humidity mean, mean battery level, and event
 * count. Output volume is bounded by NUM_LOCATIONS rather than total event
 * rate, which differs from the e-commerce job's global windowAll semantics.
 *
 * Input:  enriched-events (JSON)
 * Output: aggregated-metrics (JSON, one per location-second)
 *
 * Uses RocksDB state backend with incremental checkpoints for durability.
 */
public class IoTAggregationJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = getParam(args, "bootstrap.servers",
                "streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092");
        String inputTopic = getParam(args, "input.topic", "enriched-events");
        String outputTopic = getParam(args, "output.topic", "aggregated-metrics");

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("iot-aggregation-job")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> enriched = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "iot-enriched-events-source");

        DataStream<String> aggregated = enriched
                .keyBy(new LocationKeySelector())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .process(new LocationAggregationFn())
                .name("iot-1s-per-location-aggregation");

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

        env.execute("IoT Per-Location Aggregation Job");
    }

    public static class LocationKeySelector implements KeySelector<String, String> {
        @Override
        public String getKey(String value) {
            String pattern = "\"location_id\":";
            int start = value.indexOf(pattern);
            if (start < 0) return "unknown";
            start += pattern.length();
            int end = start;
            while (end < value.length() && (Character.isDigit(value.charAt(end)) || value.charAt(end) == '-')) {
                end++;
            }
            if (end == start) return "unknown";
            return value.substring(start, end);
        }
    }

    /**
     * Process the buffered events at window close to compute mean / max / p99
     * temperature plus mean humidity + battery + event count. ProcessWindowFunction
     * (rather than AggregateFunction) is required because percentiles are not
     * incremental; this is acceptable because each location only carries 10-30
     * events per 1-second window at the campaign load.
     */
    public static class LocationAggregationFn
            extends ProcessWindowFunction<String, String, String, TimeWindow> {

        @Override
        public void process(String locationKey, Context ctx,
                            Iterable<String> events, Collector<String> out) {
            List<Double> temps = new ArrayList<>();
            double humSum = 0.0;
            double battSum = 0.0;
            int count = 0;
            for (String evt : events) {
                Double t = extractDouble(evt, "temperature");
                Double h = extractDouble(evt, "humidity");
                Double b = extractDouble(evt, "battery_level");
                if (t != null) temps.add(t);
                if (h != null) humSum += h;
                if (b != null) battSum += b;
                count++;
            }
            if (count == 0 || temps.isEmpty()) return;

            Collections.sort(temps);
            double tMean = mean(temps);
            double tMax = temps.get(temps.size() - 1);
            double tP99 = percentile(temps, 99);
            double hMean = humSum / count;
            double bMean = battSum / count;

            String json = String.format(
                    "{\"location_id\":%s,\"window_start\":%d,\"window_end\":%d,"
                            + "\"event_count\":%d,"
                            + "\"temperature_mean\":%.3f,\"temperature_max\":%.3f,\"temperature_p99\":%.3f,"
                            + "\"humidity_mean\":%.3f,\"battery_mean\":%.3f,"
                            + "\"emit_ts\":%d}",
                    locationKey,
                    ctx.window().getStart(),
                    ctx.window().getEnd(),
                    count, tMean, tMax, tP99, hMean, bMean,
                    System.currentTimeMillis()
            );
            out.collect(json);
        }

        private double mean(List<Double> xs) {
            double s = 0.0;
            for (double x : xs) s += x;
            return s / xs.size();
        }

        private double percentile(List<Double> sortedXs, double p) {
            int idx = (int) Math.ceil(p / 100.0 * sortedXs.size()) - 1;
            if (idx < 0) idx = 0;
            if (idx >= sortedXs.size()) idx = sortedXs.size() - 1;
            return sortedXs.get(idx);
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
