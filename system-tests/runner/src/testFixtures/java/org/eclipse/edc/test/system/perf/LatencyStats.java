package org.eclipse.edc.test.system.perf;

import java.util.Arrays;
import java.util.Collection;

/**
 * Descriptive statistics over a series of durations, in milliseconds. Percentiles use the
 * nearest-rank method (no interpolation): p50 of [1, 2, 3, 4] is 2, p100 is always the max.
 */
public record LatencyStats(int count, double minMs, double meanMs, double p50Ms, double p95Ms, double p99Ms,
                           double maxMs, double stdDevMs) {

    private static final double NANOS_PER_MS = 1_000_000.0;

    public static LatencyStats ofNanos(Collection<Long> nanos) {
        if (nanos.isEmpty()) {
            return new LatencyStats(0, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN);
        }
        var sorted = nanos.stream().mapToDouble(n -> n / NANOS_PER_MS).sorted().toArray();
        var mean = Arrays.stream(sorted).average().orElseThrow();
        var variance = Arrays.stream(sorted).map(v -> (v - mean) * (v - mean)).sum() / sorted.length;
        return new LatencyStats(sorted.length, sorted[0], mean,
                percentile(sorted, 50), percentile(sorted, 95), percentile(sorted, 99),
                sorted[sorted.length - 1], Math.sqrt(variance));
    }

    static double percentile(double[] sorted, double percentile) {
        var rank = (int) Math.ceil(percentile / 100.0 * sorted.length);
        return sorted[Math.max(0, Math.min(sorted.length, rank) - 1)];
    }
}
