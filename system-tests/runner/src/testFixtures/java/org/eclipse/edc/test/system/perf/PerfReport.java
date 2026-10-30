package org.eclipse.edc.test.system.perf;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Collects every sample of a run and writes two files to the output directory:
 * <ul>
 *     <li>{@code samples-<target>-<timestamp>.csv}: one row per call, raw, for later analysis;</li>
 *     <li>{@code summary-<target>-<timestamp>.json}: statistics per series (phase, path, size),
 *     warmup and failed calls excluded, plus the overhead of each path over the direct one.</li>
 * </ul>
 */
public class PerfReport {

    public static final String PHASE_STEADY_STATE = "after-negotiation";
    public static final String PATH_DIRECT = "direct";

    private static final DateTimeFormatter FILE_TIMESTAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss").withZone(ZoneOffset.UTC);
    private static final String CSV_HEADER = "target,phase,path,size_bytes,iteration,warmup,ok,status,bytes,ttfb_ms,total_ms,error";

    private final String target;
    private final Path outputDir;
    private final Instant startedAt = Instant.now();
    private final List<Sample> samples = Collections.synchronizedList(new ArrayList<>());

    public PerfReport(String target, Path outputDir) {
        this.target = target;
        this.outputDir = outputDir;
    }

    /**
     * One measured call or step.
     *
     * @param sizeBytes requested payload size, -1 when not applicable
     * @param ok        whether the call had the expected outcome (status, size); only ok, non-warmup
     *                  samples count in the statistics
     */
    public record Sample(String phase, String path, long sizeBytes, int iteration, boolean warmup, boolean ok,
                         int status, long bytes, long ttfbNanos, long totalNanos, String error) {
    }

    /**
     * Statistics of one series. {@code total} is until the last byte, {@code ttfb} until the
     * response headers.
     */
    public record SeriesSummary(String phase, String path, long sizeBytes, int samples, int failures,
                                LatencyStats total, LatencyStats ttfb) {
    }

    /**
     * Overhead of a path over the direct one at the same size, in milliseconds.
     */
    public record Overhead(String path, long sizeBytes, double p50Ms, double p95Ms, double meanMs) {
    }

    public void add(Sample sample) {
        samples.add(sample);
    }

    public List<SeriesSummary> summaries() {
        Map<List<Object>, List<Sample>> series;
        synchronized (samples) {
            series = samples.stream().filter(s -> !s.warmup())
                    .collect(Collectors.groupingBy(s -> List.of(s.phase(), s.path(), s.sizeBytes()), LinkedHashMap::new, Collectors.toList()));
        }
        return series.values().stream().map(list -> {
            var first = list.get(0);
            var ok = list.stream().filter(Sample::ok).toList();
            return new SeriesSummary(first.phase(), first.path(), first.sizeBytes(), ok.size(), list.size() - ok.size(),
                    LatencyStats.ofNanos(ok.stream().map(Sample::totalNanos).toList()),
                    LatencyStats.ofNanos(ok.stream().map(Sample::ttfbNanos).toList()));
        }).toList();
    }

    public List<Overhead> overheads() {
        var steady = summaries().stream().filter(s -> PHASE_STEADY_STATE.equals(s.phase())).toList();
        var result = new ArrayList<Overhead>();
        for (var series : steady) {
            if (PATH_DIRECT.equals(series.path())) {
                continue;
            }
            steady.stream()
                    .filter(s -> PATH_DIRECT.equals(s.path()) && s.sizeBytes() == series.sizeBytes())
                    .findFirst()
                    .ifPresent(direct -> result.add(new Overhead(series.path(), series.sizeBytes(),
                            series.total().p50Ms() - direct.total().p50Ms(),
                            series.total().p95Ms() - direct.total().p95Ms(),
                            series.total().meanMs() - direct.total().meanMs())));
        }
        return result;
    }

    /**
     * Writes the CSV and JSON files and returns their paths.
     */
    public List<Path> write(Map<String, Object> metadata) {
        try {
            Files.createDirectories(outputDir);
            var suffix = "%s-%s".formatted(target, FILE_TIMESTAMP.format(startedAt));
            var csv = outputDir.resolve("samples-%s.csv".formatted(suffix));
            var json = outputDir.resolve("summary-%s.json".formatted(suffix));

            var lines = new ArrayList<String>();
            lines.add(CSV_HEADER);
            synchronized (samples) {
                samples.forEach(s -> lines.add(toCsv(s)));
            }
            Files.write(csv, lines);

            var document = new LinkedHashMap<String, Object>();
            document.put("target", target);
            document.put("startedAt", startedAt.toString());
            document.put("finishedAt", Instant.now().toString());
            document.put("metadata", metadata);
            document.put("series", summaries());
            document.put("overheadOverDirect", overheads());
            new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT).writeValue(json.toFile(), document);
            return List.of(csv, json);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot write the performance report to " + outputDir, e);
        }
    }

    /**
     * Human-readable table of the summaries, for the test log.
     */
    public String table() {
        var builder = new StringBuilder();
        builder.append("%-18s %-16s %10s %5s %4s %9s %9s %9s %9s %9s%n"
                .formatted("phase", "path", "size", "n", "ko", "p50 ms", "p95 ms", "p99 ms", "mean ms", "ttfb p50"));
        for (var s : summaries()) {
            builder.append("%-18s %-16s %10s %5d %4d %9.1f %9.1f %9.1f %9.1f %9.1f%n".formatted(
                    s.phase(), s.path(), s.sizeBytes() < 0 ? "-" : String.valueOf(s.sizeBytes()), s.samples(), s.failures(),
                    s.total().p50Ms(), s.total().p95Ms(), s.total().p99Ms(), s.total().meanMs(), s.ttfb().p50Ms()));
        }
        for (var o : overheads()) {
            builder.append("overhead of %-16s over direct at %10s bytes: p50 %+.1f ms, p95 %+.1f ms, mean %+.1f ms%n"
                    .formatted(o.path(), o.sizeBytes() < 0 ? "-" : String.valueOf(o.sizeBytes()), o.p50Ms(), o.p95Ms(), o.meanMs()));
        }
        return builder.toString();
    }

    private String toCsv(Sample s) {
        return String.join(",",
                target, s.phase(), s.path(), String.valueOf(s.sizeBytes()), String.valueOf(s.iteration()),
                String.valueOf(s.warmup()), String.valueOf(s.ok()), String.valueOf(s.status()), String.valueOf(s.bytes()),
                String.format(Locale.ROOT, "%.3f", s.ttfbNanos() / 1_000_000.0), String.format(Locale.ROOT, "%.3f", s.totalNanos() / 1_000_000.0),
                csvEscape(Objects.toString(s.error(), "")));
    }

    private static String csvEscape(String value) {
        if (value.contains(",") || value.contains("\"") || value.contains("\n")) {
            return "\"" + value.replace("\"", "\"\"") + "\"";
        }
        return value;
    }
}
