package com.sentinelflow.simulator;

import com.sentinelflow.simulator.model.TermoDataRecord;
import com.sentinelflow.simulator.reader.CsvReader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class CsvReaderTest {

    @Test
    void shouldParseTermoCsv(@TempDir Path tempDir) throws Exception {
        Path csv = tempDir.resolve("termo.csv");
        Files.writeString(csv, """
                packet_id,event_time,tr_id,unit_id,sensor_id,val,status
                -5517330598926187525,2025-11-10 00:00:05.000000,129222,880833,851208,5,1
                -5517330598926187526,2025-11-10 00:00:06.000000,129223,880834,851209,10,0
                """.stripIndent());

        var records = CsvReader.read(csv.toString(), fields -> new TermoDataRecord(
                Long.parseLong(fields[0]),
                LocalDateTime.parse(fields[1].replace(" ", "T")),
                Long.parseLong(fields[2]),
                Long.parseLong(fields[3]),
                Long.parseLong(fields[4]),
                Double.parseDouble(fields[5]),
                Integer.parseInt(fields[6])
        ));

        assertThat(records).hasSize(2);
        assertThat(records.getFirst().packetId()).isEqualTo(-5517330598926187525L);
        assertThat(records.get(0).val()).isEqualTo(5.0);
        assertThat(records.get(0).status()).isEqualTo(1);
        assertThat(records.get(1).val()).isEqualTo(10.0);
    }

    @Test
    void shouldSkipMalformedLines(@TempDir Path tempDir) throws Exception {
        Path csv = tempDir.resolve("bad.csv");
        Files.writeString(csv, """
                packet_id,val
                good,42
                bad
                """.stripIndent());

        var records = CsvReader.read(csv.toString(), fields -> {
            if (fields.length < 2) return null;
            if ("bad".equals(fields[0])) return null;
            return fields;
        });

        assertThat(records).hasSize(1);
    }

    @Test
    void shouldReturnEmptyForMissingFile() {
        var records = CsvReader.read("/nonexistent/file.csv", fields -> "");
        assertThat(records).isEmpty();
    }
}
