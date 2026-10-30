package org.eclipse.edc.test.system.perf;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.eclipse.edc.test.system.perf.PerfReport.Sample;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.test.system.perf.PerfReport.PATH_DIRECT;
import static org.eclipse.edc.test.system.perf.PerfReport.PHASE_STEADY_STATE;

class PerfReportTest {

    private static final long MS = 1_000_000L;

    @Test
    void summaries_excludeWarmupAndCountFailuresSeparately() {
        var report = new PerfReport("playground", Path.of("unused"));
        report.add(sample(PATH_DIRECT, 1024, true, true, 500));
        report.add(sample(PATH_DIRECT, 1024, false, true, 10));
        report.add(sample(PATH_DIRECT, 1024, false, true, 20));
        report.add(sample(PATH_DIRECT, 1024, false, false, 9_000));

        var summary = report.summaries().get(0);

        assertThat(summary.samples()).isEqualTo(2);
        assertThat(summary.failures()).isEqualTo(1);
        assertThat(summary.total().maxMs()).isEqualTo(20.0);
    }

    @Test
    void overheads_differenceWithDirectAtSameSize() {
        var report = new PerfReport("playground", Path.of("unused"));
        report.add(sample(PATH_DIRECT, 1024, false, true, 10));
        report.add(sample("consumer-data", 1024, false, true, 35));
        report.add(sample(PATH_DIRECT, 2048, false, true, 12));
        report.add(sample("consumer-data", 2048, false, true, 40));

        assertThat(report.overheads()).containsExactly(
                new PerfReport.Overhead("consumer-data", 1024, 25.0, 25.0, 25.0),
                new PerfReport.Overhead("consumer-data", 2048, 28.0, 28.0, 28.0));
    }

    @Test
    void write_csvUsesDotDecimalsWhateverTheLocale(@TempDir Path dir) throws IOException {
        var defaultLocale = Locale.getDefault();
        Locale.setDefault(Locale.FRANCE);
        try {
            var report = new PerfReport("playground", dir);
            report.add(new Sample(PHASE_STEADY_STATE, PATH_DIRECT, 1024, 0, false, false, -1, 0, 1_500_000, 2_250_000, "IOException: a, b"));

            var files = report.write(Map.of("runs", 1));

            var csv = Files.readAllLines(files.get(0));
            assertThat(csv).hasSize(2);
            assertThat(csv.get(1)).isEqualTo("playground,after-negotiation,direct,1024,0,false,false,-1,0,1.500,2.250,\"IOException: a, b\"");
            var json = new ObjectMapper().readTree(files.get(1).toFile());
            assertThat(json.get("target").asText()).isEqualTo("playground");
            assertThat(json.get("series").get(0).get("failures").asInt()).isEqualTo(1);
        } finally {
            Locale.setDefault(defaultLocale);
        }
    }

    private static Sample sample(String path, long size, boolean warmup, boolean ok, long millis) {
        return new Sample(PHASE_STEADY_STATE, path, size, 0, warmup, ok, ok ? 200 : 500, size, millis * MS / 2, millis * MS, null);
    }
}
