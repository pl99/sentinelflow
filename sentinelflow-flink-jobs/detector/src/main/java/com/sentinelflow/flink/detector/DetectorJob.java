package com.sentinelflow.flink.detector;

import com.sentinelflow.flink.serialization.FlinkSerialization;
import com.sentinelflow.flink.serialization.JacksonTypeInfo;
import com.sentinelflow.common.config.KafkaTopics;
import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.common.event.EnrichedTelemetryEvent;
import com.sentinelflow.common.event.TelemetryEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

public class DetectorJob {

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = args.length > 0 ? args[0] : "localhost:9092";

        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/flink-checkpoints");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(Duration.ofSeconds(30).toMillis());
        env.setParallelism(2);

        // --- enriched-events source for StatisticalDetector ---
        KafkaSource<EnrichedTelemetryEvent> enrichedSource = KafkaSource.<EnrichedTelemetryEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(KafkaTopics.ENRICHED_EVENTS)
                .setGroupId("flink-detector-enriched")
                .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest())
                .setDeserializer(FlinkSerialization.deserializer(EnrichedTelemetryEvent.class))
                .build();

        DataStream<EnrichedTelemetryEvent> enrichedInput = env.fromSource(
                enrichedSource, WatermarkStrategy.noWatermarks(), "kafka-enriched-events");

        DataStream<AnomalyEvent> statisticalAnomalies = enrichedInput
                .filter(e -> "metric".equals(e.original().type()))
                .keyBy(e -> e.original().source() + ":" + subMetricName(e.original()))
                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
                .process(new StatisticalDetector())
                .returns(new JacksonTypeInfo<>(AnomalyEvent.class))
                .name("statistical-detection");

        // --- raw-events source for RuleEngineFunction ---
        KafkaSource<TelemetryEvent> rawSource = KafkaSource.<TelemetryEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(KafkaTopics.RAW_EVENTS)
                .setGroupId("flink-detector")
                .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest())
                .setDeserializer(FlinkSerialization.deserializer(TelemetryEvent.class))
                .build();

        DataStream<TelemetryEvent> rawInput = env.fromSource(
                rawSource, WatermarkStrategy.noWatermarks(), "kafka-raw-events");

        DataStream<AnomalyEvent> ruleAnomalies = rawInput
                .process(new RuleEngineFunction())
                .returns(new JacksonTypeInfo<>(AnomalyEvent.class))
                .name("rule-engine");

        // --- union both streams ---
        DataStream<AnomalyEvent> anomalies = statisticalAnomalies.union(ruleAnomalies);

        var recordSerializer = KafkaRecordSerializationSchema.<AnomalyEvent>builder()
                .setTopic(KafkaTopics.ANOMALY_EVENTS)
                .setValueSerializationSchema(FlinkSerialization.serializer(AnomalyEvent.class))
                .build();

        KafkaSink<AnomalyEvent> sink = KafkaSink.<AnomalyEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(recordSerializer)
                .build();

        anomalies.sinkTo(sink).name("kafka-anomaly-events");
        env.execute("SentinelFlow Detector Job");
    }

    static String subMetricName(TelemetryEvent e) {
        if (e.payload() == null) return "unknown";
        Object name = e.payload().get("metric");
        return name != null ? name.toString() : (e.subtype() != null ? e.subtype() : "unknown");
    }
}
