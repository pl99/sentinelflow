package com.sentinelflow.llm.config;

import com.sentinelflow.llm.client.OllamaClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.time.Duration;

@Configuration
public class LlmConfig {

    @Value("${sentinelflow.llm.ollama.url}")
    private String ollamaUrl;

    @Value("${sentinelflow.llm.ollama.model}")
    private String model;

    @Value("${sentinelflow.llm.ollama.timeout}")
    private Duration timeout;

    @Bean
    public OllamaClient ollamaClient() {
        return new OllamaClient(ollamaUrl, model, timeout);
    }

    @Bean
    public ThreadPoolTaskExecutor llmExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(1);
        executor.setMaxPoolSize(2);
        executor.setQueueCapacity(20);
        executor.setThreadNamePrefix("llm-worker-");
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.initialize();
        return executor;
    }
}
