package com.sentinelflow.llm.prompt;

public final class PromptTemplates {

    private PromptTemplates() {}

    public static String resolveLanguage(String lang) {
        if (lang == null) return "English";
        return switch (lang.toLowerCase()) {
            case "en" -> "English";
            case "ru" -> "Russian";
            case "de" -> "German";
            case "fr" -> "French";
            case "es" -> "Spanish";
            case "it" -> "Italian";
            case "pt" -> "Portuguese";
            case "zh" -> "Chinese";
            case "ja" -> "Japanese";
            case "ko" -> "Korean";
            default -> lang;
        };
    }

    public static String anomalyInterpretation(String language, String service, String metric, double score,
                                               String severity, String description, String details) {
        return """
                You are an SRE expert analyzing a system anomaly. Answer in %s.

                SERVICE: %s
                METRIC: %s
                SEVERITY: %s
                ANOMALY SCORE: %.2f
                DESCRIPTION: %s
                DETAILS: %s

                Respond with a JSON object containing these fields:
                - "interpretation": what this anomaly means in plain language (in %s)
                - "classification": one of LatencyIssue, ErrorSpike, ThroughputDegradation, ResourceExhaustion, Unknown (in English)
                - "probableCauses": array of 2-3 likely root causes (in %s)
                - "recommendations": array of 1-2 immediate actions to investigate (in %s)
                """
                .formatted(language, service, metric, severity, score, description, details,
                        language, language, language);
    }

    public static String logInterpretation(String language, String source, String level, String message) {
        return """
                You are a systems analyst. Interpret this log entry. Answer in %s.

                SOURCE: %s
                LEVEL: %s
                MESSAGE: %s

                Respond with a JSON object containing these fields:
                - "impact": what is the likely impact on the system (in %s)
                - "escalate": YES or NO (in English)
                - "componentToCheck": what component should be checked first (in %s)
                """
                .formatted(language, source, level, message, language, language);
    }
}
