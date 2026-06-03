package com.sentinelflow.simulator.config;

import com.sentinelflow.simulator.service.AnomalyGeneratorService;
import com.sentinelflow.simulator.service.IngestionClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SimulatorConfig {

    private static final Logger log = LoggerFactory.getLogger(SimulatorConfig.class);

    @Value("${simulator.ingestion.url:http://localhost:8081/api/v1/ingest}")
    private String ingestionUrl;

    @Value("${simulator.delay-ms:100}")
    private long delayMs;

    @Value("${simulator.auto-start:false}")
    private boolean autoStart;

    @Value("${simulator.anomaly.enabled:false}")
    private boolean anomalyEnabled;

    @Value("${simulator.anomaly.cycles:0}")
    private int anomalyCycles;

    @Value("${simulator.anomaly.continuous:false}")
    private boolean anomalyContinuous;

    @Bean
    public IngestionClient ingestionClient() {
        return new IngestionClient(ingestionUrl);
    }

    @Bean
    public AnomalyGeneratorService anomalyGeneratorService(IngestionClient ingestionClient) {
        return new AnomalyGeneratorService(ingestionClient, delayMs);
    }

    @Bean
    public CommandLineRunner simulationRunner(AnomalyGeneratorService anomalyGen) {
        return args -> {
            if (autoStart) {
                if (anomalyEnabled) {
                    log.info("Anomaly generation enabled (cycles={}, continuous={})",
                            anomalyCycles, anomalyContinuous);
                    if (anomalyContinuous) {
                        anomalyGen.generateForever();
                    } else if (anomalyCycles > 0) {
                        int sent = anomalyGen.generate(anomalyCycles);
                        log.info("Anomaly generation finished: {} events sent", sent);
                    } else {
                        log.warn("anomaly.enabled=true but no cycles specified and continuous=false");
                    }
                } else {
                    log.info("Auto-start enabled but anomaly generation is disabled. Nothing to run.");
                }
            } else {
                log.info("Simulator loaded. Set simulator.auto-start=true to run on startup.");
                log.info("  delay: {}ms", delayMs);
                log.info("  ingestion URL: {}", ingestionUrl);
                log.info("  anomaly enabled: {}", anomalyEnabled);
            }
        };
    }
}
