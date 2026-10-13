package com.phonotech.streaming.enrichment;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.AvroDeserializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Flink Job 1: Event Enrichment
 *
 * Stateless job that enriches raw e-commerce events with product catalog data
 * (category, brand, base_price). EnrichmentProcessFunction builds a simulated
 * catalog of 500 products in a plain in-memory HashMap when it opens, and looks
 * up each event's product_id there. The catalog is not kept in Flink state.
 * Events with no known product_id pass through with "enriched": false.
 *
 * Input:  raw-events (JSON strings)
 * Output: enriched-events (JSON string with enrichment fields appended)
 * Error:  dead-letter (records that fail processing)
 */
public class EnrichmentJob {

    private static final String PARAM_BOOTSTRAP = "bootstrap.servers";
    private static final String PARAM_GROUP_ID = "group.id";
    private static final String PARAM_INPUT_TOPIC = "input.topic";
    private static final String PARAM_OUTPUT_TOPIC = "output.topic";
    private static final String PARAM_DLQ_TOPIC = "dlq.topic";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String bootstrapServers = getParam(args, PARAM_BOOTSTRAP, "streaming-kafka-kafka-bootstrap.streaming.svc.cluster.local:9092");
        String groupId = getParam(args, PARAM_GROUP_ID, "enrichment-job");
        String inputTopic = getParam(args, PARAM_INPUT_TOPIC, "raw-events");
        String outputTopic = getParam(args, PARAM_OUTPUT_TOPIC, "enriched-events");
        String dlqTopic = getParam(args, PARAM_DLQ_TOPIC, "dead-letter");

        // Source: raw-events
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawEvents = env.fromSource(source, WatermarkStrategy.noWatermarks(), "raw-events-source");

        // Dead letter output tag
        OutputTag<String> dlqTag = new OutputTag<String>("dead-letter") {};

        // Enrich events
        SingleOutputStreamOperator<String> enriched = rawEvents
                .process(new EnrichmentProcessFunction(dlqTag))
                .name("event-enrichment");

        // Main output -> enriched-events
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

        // Dead letter output -> dead-letter topic
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

        env.execute("Event Enrichment Job");
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
