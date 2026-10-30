package org.eclipse.edc.test.system.perf;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class LatencyStatsTest {

    private static final long MS = 1_000_000L;

    @Test
    void ofNanos_hundredValues_nearestRankPercentiles() {
        var nanos = LongStream.rangeClosed(1, 100).map(v -> v * MS).boxed().toList();

        var stats = LatencyStats.ofNanos(nanos);

        assertThat(stats.count()).isEqualTo(100);
        assertThat(stats.minMs()).isEqualTo(1.0);
        assertThat(stats.maxMs()).isEqualTo(100.0);
        assertThat(stats.meanMs()).isEqualTo(50.5);
        assertThat(stats.p50Ms()).isEqualTo(50.0);
        assertThat(stats.p95Ms()).isEqualTo(95.0);
        assertThat(stats.p99Ms()).isEqualTo(99.0);
        assertThat(stats.stdDevMs()).isCloseTo(28.866, within(0.001));
    }

    @Test
    void ofNanos_unsortedInput_sameResultAsSorted() {
        var stats = LatencyStats.ofNanos(List.of(4 * MS, 1 * MS, 3 * MS, 2 * MS));

        assertThat(stats.minMs()).isEqualTo(1.0);
        assertThat(stats.p50Ms()).isEqualTo(2.0);
        assertThat(stats.p95Ms()).isEqualTo(4.0);
        assertThat(stats.maxMs()).isEqualTo(4.0);
    }

    @Test
    void ofNanos_singleValue_everyStatisticIsThatValue() {
        var stats = LatencyStats.ofNanos(List.of(7 * MS));

        assertThat(stats.count()).isEqualTo(1);
        assertThat(List.of(stats.minMs(), stats.meanMs(), stats.p50Ms(), stats.p99Ms(), stats.maxMs())).containsOnly(7.0);
        assertThat(stats.stdDevMs()).isZero();
    }

    @Test
    void ofNanos_empty_countZeroAndNaN() {
        var stats = LatencyStats.ofNanos(List.of());

        assertThat(stats.count()).isZero();
        assertThat(stats.p50Ms()).isNaN();
    }
}
