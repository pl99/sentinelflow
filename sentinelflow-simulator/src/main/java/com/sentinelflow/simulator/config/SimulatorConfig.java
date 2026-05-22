package com.sentinelflow.simulator.config;

import com.sentinelflow.simulator.service.AnomalyGeneratorService;
import com.sentinelflow.simulator.service.DataSimulatorService;
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

    @Value("${simulator.data.termo-file:}")
    private String termoFilePath;

    @Value("${simulator.data.fuel-file:}")
    private String fuelFilePath;

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
    public DataSimulatorService dataSimulatorService(IngestionClient ingestionClient) {
        return new DataSimulatorService(ingestionClient, termoFilePath, fuelFilePath, delayMs);
    }

    @Bean
    public AnomalyGeneratorService anomalyGeneratorService(IngestionClient ingestionClient) {
        return new AnomalyGeneratorService(ingestionClient, delayMs);
    }

    @Bean
    public CommandLineRunner simulationRunner(
            DataSimulatorService simulator,
            AnomalyGeneratorService anomalyGen) {
        return args -> {
            if (autoStart) {
                log.info("Auto-starting data simulation...");
                if (termoFilePath != null && !termoFilePath.isBlank()
                        && fuelFilePath != null && !fuelFilePath.isBlank()) {
                    int sent = simulator.runSimulation();
                    log.info("CSV simulation finished: {} events sent", sent);
                } else {
                    log.info("No CSV files configured, skipping file-based simulation");
                }

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
                }
            } else {
                log.info("Simulator loaded. Set simulator.auto-start=true to run on startup.");
                log.info("  termo file: {}", termoFilePath);
                log.info("  fuel  file: {}", fuelFilePath);
                log.info("  delay: {}ms", delayMs);
                log.info("  ingestion URL: {}", ingestionUrl);
                log.info("  anomaly enabled: {}", anomalyEnabled);
            }
        };
    }
}
