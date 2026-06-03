package com.sentinelflow.storage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SentinelFlowStorageApplication {

    public static void main(String[] args) {
        SpringApplication.run(SentinelFlowStorageApplication.class, args);
    }
}
