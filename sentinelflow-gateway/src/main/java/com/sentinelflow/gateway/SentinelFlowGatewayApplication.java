package com.sentinelflow.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SentinelFlowGatewayApplication {

    public static void main(String[] args) {
        SpringApplication.run(SentinelFlowGatewayApplication.class, args);
    }
}
