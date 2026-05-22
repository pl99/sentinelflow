package com.sentinelflow.simulator.reader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public final class CsvReader {

    private static final Logger log = LoggerFactory.getLogger(CsvReader.class);

    private CsvReader() {}

    public static <T> List<T> read(String filePath, CsvRowParser<T> parser) {
        Path path = Path.of(filePath);
        if (!Files.exists(path)) {
            log.warn("File not found: {}", path.toAbsolutePath());
            return List.of();
        }
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String header = reader.readLine();
            if (header == null) {
                log.warn("Empty file: {}", path.toAbsolutePath());
                return List.of();
            }
            log.info("Reading {}, header: {}", path.toAbsolutePath(), header);
            return reader.lines()
                    .filter(line -> !line.isBlank())
                    .map(line -> {
                        try {
                            return parser.parse(line.split(",", -1));
                        } catch (Exception e) {
                            log.warn("Skipping malformed line: {}", line);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .toList();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read CSV: " + path, e);
        }
    }

    @FunctionalInterface
    public interface CsvRowParser<T> {
        T parse(String[] fields);
    }
}
