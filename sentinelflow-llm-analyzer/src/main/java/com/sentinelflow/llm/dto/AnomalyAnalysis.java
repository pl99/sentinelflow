package com.sentinelflow.llm.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record AnomalyAnalysis(
    @JsonProperty(required = true) String interpretation,
    @JsonProperty(required = true) String classification,
    @JsonProperty(required = true) List<String> probableCauses,
    @JsonProperty(required = true) List<String> recommendations,
    String severity
) {}
