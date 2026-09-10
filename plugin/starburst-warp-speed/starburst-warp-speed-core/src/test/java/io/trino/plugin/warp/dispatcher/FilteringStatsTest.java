/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.warp.dispatcher;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FilteringStatsTest
{
    private static final double LOW_SELECTIVITY_THRESHOLD = 0.001;
    private static final double CHUNK_SKIP_THRESHOLD = 0.05;
    private static final long ROWS_PER_SPLIT = 4096;
    private static final long CHUNKS_PER_SPLIT = 100;
    private static final int RESAMPLE_AFTER_PROXIED_SPLITS = 32;
    private static final int MAX_SAMPLING_SPLITS = 8;
    private static final int MIN_SAMPLE_SPLITS = 2;

    @Test
    public void testFilteringAdmittedWhileSampling()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
        recordSplit(filteringStats, ROWS_PER_SPLIT, CHUNKS_PER_SPLIT);
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
    }

    @Test
    public void testFilteringDisabledWhenTooFewChunksAreSkipped()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        recordSplit(filteringStats, ROWS_PER_SPLIT, CHUNKS_PER_SPLIT);
        recordSplit(filteringStats, ROWS_PER_SPLIT, CHUNKS_PER_SPLIT);
        assertThat(filteringStats.tryAdmitFiltering()).isFalse();
    }

    @Test
    public void testFilteringKeptWhenEnoughChunksAreSkipped()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        recordSplit(filteringStats, ROWS_PER_SPLIT, 90);
        recordSplit(filteringStats, ROWS_PER_SPLIT, 90);
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
    }

    @Test
    public void testFilteringKeptWhenSelectivityIsLow()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        recordSplit(filteringStats, 1, CHUNKS_PER_SPLIT);
        recordSplit(filteringStats, 1, CHUNKS_PER_SPLIT);
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
    }

    @Test
    public void testVerdictDeferredUntilEnoughSplitsAreSampled()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        recordSplit(filteringStats, ROWS_PER_SPLIT, CHUNKS_PER_SPLIT);
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
    }

    @Test
    public void testVerdictDeferredUntilEnoughRowsAreSampled()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        filteringStats.recordProcessed(1000, 1000, CHUNKS_PER_SPLIT, CHUNKS_PER_SPLIT);
        filteringStats.recordProcessed(1000, 1000, CHUNKS_PER_SPLIT, CHUNKS_PER_SPLIT);
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
    }

    @Test
    public void testEmptySplitsCarryNoSignal()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        filteringStats.recordProcessed(0, 0, 0, 0);
        filteringStats.recordProcessed(0, 0, 0, 0);
        recordSplit(filteringStats, ROWS_PER_SPLIT, CHUNKS_PER_SPLIT);
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
    }

    @Test
    public void testFilteringRetriedAfterEnoughProxiedSplits()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        recordSplit(filteringStats, ROWS_PER_SPLIT, CHUNKS_PER_SPLIT);
        recordSplit(filteringStats, ROWS_PER_SPLIT, CHUNKS_PER_SPLIT);
        for (int split = 0; split < RESAMPLE_AFTER_PROXIED_SPLITS - 1; split++) {
            assertThat(filteringStats.tryAdmitFiltering()).isFalse();
        }
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
    }

    @Test
    public void testSamplingAdmitsABoundedNumberOfSplitsAtOnce()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        for (int split = 0; split < MAX_SAMPLING_SPLITS; split++) {
            assertThat(filteringStats.tryAdmitFiltering()).isTrue();
        }
        assertThat(filteringStats.tryAdmitFiltering()).isFalse();
    }

    @Test
    public void testSampledSplitFreesItsSamplingSlot()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        for (int split = 0; split < MAX_SAMPLING_SPLITS; split++) {
            assertThat(filteringStats.tryAdmitFiltering()).isTrue();
        }
        recordSplit(filteringStats, 1, CHUNKS_PER_SPLIT);
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
    }

    @Test
    public void testSplitWithoutASampleFreesItsSamplingSlot()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        for (int split = 0; split < MAX_SAMPLING_SPLITS; split++) {
            assertThat(filteringStats.tryAdmitFiltering()).isTrue();
        }
        filteringStats.releaseSamplingSlot();
        assertThat(filteringStats.tryAdmitFiltering()).isTrue();
        assertThat(filteringStats.tryAdmitFiltering()).isFalse();
    }

    @Test
    public void testEnabledFilteringIgnoresTheSamplingBound()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        recordSplit(filteringStats, 1, CHUNKS_PER_SPLIT);
        recordSplit(filteringStats, 1, CHUNKS_PER_SPLIT);
        for (int split = 0; split < MAX_SAMPLING_SPLITS + 1; split++) {
            assertThat(filteringStats.tryAdmitFiltering()).isTrue();
        }
    }

    @Test
    public void testRetryAfterDisabledSamplesFewerSplitsAtOnce()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        recordSplit(filteringStats, ROWS_PER_SPLIT, CHUNKS_PER_SPLIT);
        recordSplit(filteringStats, ROWS_PER_SPLIT, CHUNKS_PER_SPLIT);
        for (int split = 0; split < RESAMPLE_AFTER_PROXIED_SPLITS - 1; split++) {
            assertThat(filteringStats.tryAdmitFiltering()).isFalse();
        }
        for (int split = 0; split < MIN_SAMPLE_SPLITS; split++) {
            assertThat(filteringStats.tryAdmitFiltering()).isTrue();
        }
        assertThat(filteringStats.tryAdmitFiltering()).isFalse();
    }

    @Test
    public void testRowsAfterFilteringAboveTotalRowsIsRejected()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        assertThatThrownBy(() -> filteringStats.recordProcessed(10, 11, 1, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testMatchedChunksAboveTotalChunksIsRejected()
    {
        FilteringStats filteringStats = new FilteringStats(LOW_SELECTIVITY_THRESHOLD, CHUNK_SKIP_THRESHOLD);
        assertThatThrownBy(() -> filteringStats.recordProcessed(10, 10, 1, 2))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testThresholdsOutOfRangeAreRejected()
    {
        assertThatThrownBy(() -> new FilteringStats(1.5, CHUNK_SKIP_THRESHOLD))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FilteringStats(LOW_SELECTIVITY_THRESHOLD, -0.1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static void recordSplit(FilteringStats filteringStats, long rowsAfterFiltering, long matchedChunks)
    {
        filteringStats.recordProcessed(ROWS_PER_SPLIT, rowsAfterFiltering, CHUNKS_PER_SPLIT, matchedChunks);
    }
}
