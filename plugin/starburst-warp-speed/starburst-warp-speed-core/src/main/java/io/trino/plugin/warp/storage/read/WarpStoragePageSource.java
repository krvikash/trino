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

import io.trino.spi.connector.SourcePage;

import static com.google.common.base.Preconditions.checkArgument;

public interface WarpStoragePageSource
{
    RowRanges getSortedRowRanges();

    boolean isRowsLimitReached();

    SourcePage getNextSourcePage();

    long getMemoryUsage();

    boolean isFinished();

    long getCompletedPositions();

    long getCompletedBytes();

    void close();

    final class RowRanges
    {
        public static final RowRanges EMPTY = new RowRanges(new long[0], new long[0], 0);

        private final long[] lowerInclusive;
        private final long[] upperExclusive;
        private final long rowCount;

        public RowRanges(long[] lowerInclusive, long[] upperExclusive, long rowCount)
        {
            checkArgument(
                    lowerInclusive.length == upperExclusive.length,
                    "lowerInclusive size %s should match upperExclusive size %s",
                    lowerInclusive.length,
                    upperExclusive.length);
            this.lowerInclusive = lowerInclusive;
            this.upperExclusive = upperExclusive;
            this.rowCount = rowCount;
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
    }
}
