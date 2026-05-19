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

import it.unimi.dsi.fastutil.longs.LongArrayList;

import static com.google.common.base.Preconditions.checkArgument;

public class RangeData
{
    private final LongArrayList lowerInclusive;
    private final LongArrayList upperExclusive;
    private long rowCount;

    public RangeData()
    {
        // ranges are gathered only if needed (mixed query)
        this.upperExclusive = new LongArrayList();
        this.lowerInclusive = new LongArrayList();
    }

    public void addRange(long lowerBound, int upperBound)
    {
        checkArgument(lowerBound >= 0, "lowerInclusive %s must not be negative", lowerBound);
        checkArgument(
                upperBound > lowerBound,
                "upperExclusive %s must be higher than lowerInclusive %s",
                upperBound,
                lowerBound);

        rowCount += upperBound - lowerBound;
        if (!upperExclusive.isEmpty()) {
            checkArgument(
                    lowerBound >= upperExclusive.getLast(),
                    "lowerInclusive %s must be greater than previous upperExclusive %s",
                    lowerBound,
                    upperExclusive.getLast());
            if (lowerBound == upperExclusive.getLast()) { // merge ranges
                upperExclusive.set(upperExclusive.size() - 1, upperBound);
                return;
            }
        }
        lowerInclusive.add(lowerBound);
        upperExclusive.add(upperBound);
    }

    public long[] getLowerInclusiveAsArray()
    {
        return this.lowerInclusive.toLongArray();
    }

    public long[] getUpperExclusiveAsArray()
    {
        return this.upperExclusive.toLongArray();
    }

    public long getRowCount()
    {
        return rowCount;
    }
}
