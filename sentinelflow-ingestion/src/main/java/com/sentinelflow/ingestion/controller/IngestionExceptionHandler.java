package com.sentinelflow.ingestion.controller;

import com.sentinelflow.ingestion.kafka.KafkaPublishException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * Translates ingestion-specific exceptions into appropriate HTTP responses.
 */
@RestControllerAdvice(basePackageClasses = IngestionController.class)
public class IngestionExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(IngestionExceptionHandler.class);

    @ExceptionHandler(KafkaPublishException.class)
    public ResponseEntity<Map<String, Object>> handleKafkaPublish(KafkaPublishException e) {
        log.warn("Kafka publish failed: {}", e.getMessage());
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of(
                        "accepted", false,
                        "error", "upstream temporarily unavailable, please retry"
                ));
    }
}