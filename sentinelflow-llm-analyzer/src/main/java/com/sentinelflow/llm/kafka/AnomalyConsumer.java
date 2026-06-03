package com.sentinelflow.llm.kafka;

import com.sentinelflow.common.config.KafkaTopics;
import com.sentinelflow.common.event.AnomalyEvent;
import com.sentinelflow.common.event.LlmInsight;
import com.sentinelflow.llm.service.AnomalyInterpretationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

@Component
public class AnomalyConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnomalyConsumer.class);

    private final AnomalyInterpretationService interpretationService;
    private final KafkaTemplate<String, LlmInsight> kafkaTemplate;
    private final ThreadPoolTaskExecutor executor;

    public AnomalyConsumer(AnomalyInterpretationService interpretationService,
                           KafkaTemplate<String, LlmInsight> kafkaTemplate,
                           ThreadPoolTaskExecutor llmExecutor) {
        this.interpretationService = interpretationService;
        this.kafkaTemplate = kafkaTemplate;
        this.executor = llmExecutor;
    }

    @KafkaListener(
            topics = KafkaTopics.ANOMALY_EVENTS,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(AnomalyEvent anomaly, Acknowledgment ack) {
        executor.submit(() -> process(anomaly, ack));
    }

    private void process(AnomalyEvent anomaly, Acknowledgment ack) {
        log.info("Processing anomaly: {} / {} (score={})", anomaly.service(), anomaly.metric(), anomaly.score());

        LlmInsight insight = interpretationService.analyze(anomaly);

        kafkaTemplate.send(KafkaTopics.LLM_INSIGHTS, anomaly.id(), insight)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send insight for anomaly {}: {}", anomaly.id(), ex.getMessage());
                    } else {
                        ack.acknowledge();
                        log.info("Published insight for anomaly {}", anomaly.id());
                    }
                });
    }
}
