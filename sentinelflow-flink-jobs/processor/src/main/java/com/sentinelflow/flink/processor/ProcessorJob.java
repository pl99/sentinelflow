package com.sentinelflow.flink.processor;

import com.sentinelflow.flink.processor.serialization.FlinkSerialization;
import com.sentinelflow.flink.processor.serialization.JacksonTypeInfo;
import com.sentinelflow.common.config.KafkaTopics;
import com.sentinelflow.common.event.EnrichedTelemetryEvent;
import com.sentinelflow.common.event.TelemetryEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class ProcessorJob {

    public static void main(String[] args) throws Exception {
        String kafkaBootstrap = args.length > 0 ? args[0] : "kafka:9094";

        Configuration conf = new Configuration();
        conf.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        conf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/flink-checkpoints");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.enableCheckpointing(Duration.ofSeconds(30).toMillis());
        env.setParallelism(2);

        KafkaSource<TelemetryEvent> source = KafkaSource.<TelemetryEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setTopics(KafkaTopics.RAW_EVENTS)
                .setGroupId("flink-processor")
                .setStartingOffsets(org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest())
                .setDeserializer(FlinkSerialization.deserializer(TelemetryEvent.class))
                .build();

        DataStream<TelemetryEvent> input = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-raw-events");

        DataStream<EnrichedTelemetryEvent> enriched = input
                .filter(e -> e.correlationId() != null && !e.correlationId().isBlank())
                .keyBy(TelemetryEvent::correlationId)
                .window(org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new CorrelationWindowFunction())
                .returns(new JacksonTypeInfo<>(EnrichedTelemetryEvent.class))
                .name("correlation-window");

        var recordSerializer = KafkaRecordSerializationSchema.<EnrichedTelemetryEvent>builder()
                .setTopic(KafkaTopics.ENRICHED_EVENTS)
                .setValueSerializationSchema(FlinkSerialization.<EnrichedTelemetryEvent>serializer())
                .build();

        KafkaSink<EnrichedTelemetryEvent> sink = KafkaSink.<EnrichedTelemetryEvent>builder()
                .setBootstrapServers(kafkaBootstrap)
                .setRecordSerializer(recordSerializer)
                .build();

        enriched.sinkTo(sink).name("kafka-enriched-events");
        env.execute("SentinelFlow Processor Job");
    }
}
