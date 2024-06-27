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
package io.trino.plugin.warp.storage.read;

import io.trino.spi.Page;

import static com.google.common.base.Preconditions.checkArgument;

public interface WarpStoragePageSource
{
    RowRanges getSortedRowRanges();

    boolean isRowsLimitReached();

    Page getNextPage();

    long getMemoryUsage();

    boolean isFinished();

    long getCompletedPositions();

    long getCompletedBytes();

    void close();

    final class RowRanges
    {
        public static final RowRanges EMPTY = new RowRanges(new long[0], new long[0], true);

        private final long[] lowerInclusive;
        private final long[] upperExclusive;
        private final long rowCount;
        private final boolean noMoreRowRanges;

        public RowRanges(long[] lowerInclusive, long[] upperExclusive, boolean noMoreRowRanges)
        {
            checkArgument(
                    lowerInclusive.length == upperExclusive.length,
                    "lowerInclusive size %s should match upperExclusive size %s",
                    lowerInclusive.length,
                    upperExclusive.length);
            this.lowerInclusive = lowerInclusive;
            this.upperExclusive = upperExclusive;
            this.noMoreRowRanges = noMoreRowRanges;
            long rangesRowCount = 0;
            for (int rangeIndex = 0; rangeIndex < lowerInclusive.length; rangeIndex++) {
                checkArgument(lowerInclusive[rangeIndex] >= 0, "lowerInclusive %s must not be negative", lowerInclusive[rangeIndex]);
                checkArgument(
                        upperExclusive[rangeIndex] > lowerInclusive[rangeIndex],
                        "upperExclusive %s must be higher than lowerInclusive %s",
                        upperExclusive[rangeIndex],
                        lowerInclusive[rangeIndex]);
                if (rangeIndex > 0) {
                    checkArgument(
                            lowerInclusive[rangeIndex] > upperExclusive[rangeIndex - 1],
                            "lowerInclusive %s must be greater than previous upperExclusive %s",
                            lowerInclusive[rangeIndex],
                            upperExclusive[rangeIndex - 1]);
                }
                rangesRowCount += upperExclusive[rangeIndex] - lowerInclusive[rangeIndex];
            }
            this.rowCount = rangesRowCount;
        }

        public long getLowerInclusive(int rangeIndex)
        {
            return lowerInclusive[rangeIndex];
        }

        public long getUpperExclusive(int rangeIndex)
        {
            return upperExclusive[rangeIndex];
        }

        public long getRowCount()
        {
            return rowCount;
        }

        public int getRangesCount()
        {
            return lowerInclusive.length;
        }

        public boolean isNoMoreRowRanges()
        {
            return noMoreRowRanges;
        }
    }
}
