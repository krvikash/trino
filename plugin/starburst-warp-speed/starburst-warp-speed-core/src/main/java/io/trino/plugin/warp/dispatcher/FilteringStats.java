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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Controls whether the splits of a scan filter with the warp index. The decision is made once per
 * split when its page source is created and does not change within the split. Filtering stays on
 * while the scan is sampled. Once enough rows and splits are sampled, it is turned off for later
 * splits when the index skips too few chunks to pay for the match work and too many rows survive
 * for downstream savings to pay for it instead. Only a bounded number of splits sample at once, so
 * the cost of a wrong guess does not scale with how many splits the workers start in parallel.
 * After enough proxied splits the sample is reset and filtering is retried, so selectivity that
 * varies across a table is picked up. Each retry that confirms the verdict doubles the wait before
 * the next one.
 */
public class FilteringStats
{
    /**
     * Floors below which the retained row fraction is noise rather than a selectivity signal.
     */
    private static final long MIN_SAMPLE_ROWS = 2047;
    private static final int MIN_SAMPLE_SPLITS = 2;
    /**
     * Caps how many splits filter at once before the first verdict lands.
     */
    private static final int MAX_SAMPLING_SPLITS = 8;
    /**
     * Retry filtering periodically in case index selectivity improves in later regions of the table.
     */
    private static final int INITIAL_RESAMPLE_AFTER_PROXIED_SPLITS = 32;
    private static final int MAX_RESAMPLE_AFTER_PROXIED_SPLITS = 512;

    private final double lowSelectivityThreshold;
    private final double chunkSkipThreshold;

    private FilteringMode mode = FilteringMode.SAMPLING;
    private long totalRows;
    private long rowsAfterFiltering;
    private long totalChunks;
    private long matchedChunks;
    private int sampledSplits;
    private int samplingSplitsInFlight;
    private int samplingAdmissionLimit = MAX_SAMPLING_SPLITS;
    private int proxiedSplitsSinceDisabled;
    private int resampleAfterProxiedSplits = INITIAL_RESAMPLE_AFTER_PROXIED_SPLITS;
    private boolean retryingAfterDisabled;

    public FilteringStats(double lowSelectivityThreshold, double chunkSkipThreshold)
    {
        checkArgument(lowSelectivityThreshold >= 0 && lowSelectivityThreshold <= 1, "lowSelectivityThreshold must be in range [0, 1]: %s", lowSelectivityThreshold);
        checkArgument(chunkSkipThreshold >= 0 && chunkSkipThreshold <= 1, "chunkSkipThreshold must be in range [0, 1]: %s", chunkSkipThreshold);
        this.lowSelectivityThreshold = lowSelectivityThreshold;
        this.chunkSkipThreshold = chunkSkipThreshold;
    }

    /**
     * Decides whether the calling split filters with the warp index, and advances the sampling state.
     */
    public synchronized boolean tryAdmitFiltering()
    {
        return switch (mode) {
            case SAMPLING -> {
                if (samplingSplitsInFlight >= samplingAdmissionLimit) {
                    yield false;
                }
                samplingSplitsInFlight++;
                yield true;
            }
            case ENABLED -> true;
            case DISABLED -> {
                proxiedSplitsSinceDisabled++;
                if (proxiedSplitsSinceDisabled >= resampleAfterProxiedSplits) {
                    resetSample();
                    samplingSplitsInFlight++;
                    yield true;
                }
                else {
                    yield false;
                }
            }
        };
    }

    /**
     * Frees the admission slot of a split that filters but never reports a sample.
     */
    public synchronized void releaseSamplingSlot()
    {
        if (mode == FilteringMode.SAMPLING && samplingSplitsInFlight > 0) {
            samplingSplitsInFlight--;
        }
    }

    public synchronized void recordProcessed(long totalRows, long rowsAfterFiltering, long totalChunks, long matchedChunks)
    {
        checkArgument(totalRows >= rowsAfterFiltering,
                "Total rows %s is expected to be greater or equals to the number of rows after filtering %s",
                totalRows,
                rowsAfterFiltering);
        checkArgument(totalChunks >= matchedChunks,
                "Total chunks %s is expected to be greater or equals to the number of matched chunks %s",
                totalChunks,
                matchedChunks);
        releaseSamplingSlot();
        if (totalRows == 0) {
            // empty splits carry no selectivity signal
            return;
        }

        this.totalRows += totalRows;
        this.rowsAfterFiltering += rowsAfterFiltering;
        this.totalChunks += totalChunks;
        this.matchedChunks += matchedChunks;
        sampledSplits++;
        if (sampledSplits < MIN_SAMPLE_SPLITS || this.totalRows < MIN_SAMPLE_ROWS) {
            return;
        }

        if (isInefficient()) {
            if (mode != FilteringMode.DISABLED) {
                if (retryingAfterDisabled) {
                    // the retry confirmed the verdict, so wait longer before spending splits on the next one
                    resampleAfterProxiedSplits = Math.min(resampleAfterProxiedSplits * 2, MAX_RESAMPLE_AFTER_PROXIED_SPLITS);
                }
                mode = FilteringMode.DISABLED;
                proxiedSplitsSinceDisabled = 0;
            }
        }
        else {
            mode = FilteringMode.ENABLED;
            retryingAfterDisabled = false;
            resampleAfterProxiedSplits = INITIAL_RESAMPLE_AFTER_PROXIED_SPLITS;
        }
    }

    /**
     * Filtering pays off when the index skips enough chunks to save collect work, or when so few rows
     * survive that downstream savings cover the match cost even though every chunk is read.
     */
    private boolean isInefficient()
    {
        if (rowsAfterFiltering <= lowSelectivityThreshold * totalRows) {
            return false;
        }
        return (totalChunks - matchedChunks) < chunkSkipThreshold * totalChunks;
    }

    private void resetSample()
    {
        mode = FilteringMode.SAMPLING;
        totalRows = 0;
        rowsAfterFiltering = 0;
        totalChunks = 0;
        matchedChunks = 0;
        sampledSplits = 0;
        samplingSplitsInFlight = 0;
        // a retry is speculative, so let fewer splits filter at once than in the first sample
        samplingAdmissionLimit = MIN_SAMPLE_SPLITS;
        proxiedSplitsSinceDisabled = 0;
        retryingAfterDisabled = true;
    }

    @Override
    public synchronized String toString()
    {
        return toStringHelper(this)
                .add("mode", mode)
                .add("totalRows", totalRows)
                .add("rowsAfterFiltering", rowsAfterFiltering)
                .add("totalChunks", totalChunks)
                .add("matchedChunks", matchedChunks)
                .add("sampledSplits", sampledSplits)
                .add("samplingSplitsInFlight", samplingSplitsInFlight)
                .add("proxiedSplitsSinceDisabled", proxiedSplitsSinceDisabled)
                .add("resampleAfterProxiedSplits", resampleAfterProxiedSplits)
                .toString();
    }

    private enum FilteringMode
    {
        SAMPLING,
        ENABLED,
        DISABLED,
    }
}
