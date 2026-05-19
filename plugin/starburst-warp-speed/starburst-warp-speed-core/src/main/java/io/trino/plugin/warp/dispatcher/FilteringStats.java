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

import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class FilteringStats
{
    public static final float FILTERING_THRESHOLD = 0.00001f;
    public static final int MIN_STARTED_SPLITS = 4;
    public static final int MIN_PROCESSED_SPLITS = 2;

    private long totalRows;
    private long rowsAfterFiltering;
    private int startedSplits;
    private int processedSplits;

    public synchronized boolean isEfficientFiltering(DispatcherPageSourceStats dispatcherPageSourceStats)
    {
        if (startedSplits < MIN_STARTED_SPLITS) {
            return true;
        }
        else if (processedSplits < MIN_PROCESSED_SPLITS || totalRows == 0) {
            return false;
        }

        float retainedRowRatio = (float) rowsAfterFiltering / totalRows;
        if (retainedRowRatio <= FILTERING_THRESHOLD) {
            dispatcherPageSourceStats.incefficient_filtering();
            return true;
        }
        dispatcherPageSourceStats.incinefficient_filtering();
        return false;
    }

    public synchronized void recordStarted()
    {
        startedSplits++;
    }

    public synchronized void recordProcessed(long totalRows, long rowsAfterFiltering)
    {
        if (totalRows == 0) {
            // don't count empty splits as processed splits
            return;
        }
        checkArgument(totalRows >= rowsAfterFiltering,
                "Total rows %s is expected to be greater or equals to the number of rows after filtering %s",
                totalRows,
                rowsAfterFiltering);

        this.totalRows += totalRows;
        this.rowsAfterFiltering += rowsAfterFiltering;
        processedSplits++;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("totalRows", totalRows)
                .add("rowsAfterFiltering", rowsAfterFiltering)
                .add("startedSplits", startedSplits)
                .add("processedSplits", processedSplits)
                .toString();
    }
}
