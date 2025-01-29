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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.lang.foreign.MemorySegment;

@Singleton
public class NativeRangeFillerService
        implements RangeFillerService
{
    @Inject
    public NativeRangeFillerService()
    {
    }

    // return the number of rows collected in this round
    @Override
    public int add(ChunkProperties chunkProperties, QueryArgs queryArgs, AggregatorPageArgs aggregatorPageArgs, StorageCollectorService storageCollectorService)
    {
        RangeData rangeData = aggregatorPageArgs.rangeData();
        RecordIndexes recordIndexes = rangeData.getRecordIndexes();
        int numRows = chunkProperties.numRecordsInChunk();

        // in case collected count is zero, it means nothing was collected regardless of the type
        if (numRows == 0) {
            return 0;
        }

        // if we are here we have at least one row that was collected
        RecordIndexListType listType = chunkProperties.type();
        int baseRow = chunkProperties.chunkIndex() * queryArgs.chunkSize();
        boolean rangesRequired = queryArgs.queryParams().isRangesRequired();
        switch (listType) {
            case RECORD_INDEX_LIST_TYPE_ALL -> {
                if (rangesRequired) {
                    int min = baseRow + chunkProperties.startIx();
                    long minValue = mergeRanges(min, rangeData);
                    rangeData.addLowerInclusive(minValue);
                    rangeData.addUpperExclusive(min + numRows);
                }
            }
            case RECORD_INDEX_LIST_TYPE_VALUES -> {
                if (rangesRequired) {
                    // we take the rows from where we stopped last time
                    MemorySegment recordIndexesList = recordIndexes.getList();
                    int listIdx = chunkProperties.startIx();

                    // handle the first row and check if it extends that last range we already have
                    int min = baseRow + recordIndexes.getRowFromList(recordIndexesList, listIdx);
                    listIdx++;
                    int max = min + 1;
                    min = (int) mergeRanges(min, rangeData);

                    // handle all the rest of the rows
                    for (int i = 1; i < numRows; i++) {
                        int row = baseRow + recordIndexes.getRowFromList(recordIndexesList, listIdx);
                        listIdx++;
                        // if we are consectuive - we are in the same range
                        if (row == max) {
                            max++;
                            continue;
                        }

                        // close and add the current range
                        rangeData.addLowerInclusive(min);
                        rangeData.addUpperExclusive(max);
                        // open the next range
                        min = row;
                        max = row + 1;
                    }

                    // add the last range we have
                    rangeData.addLowerInclusive(min);
                    rangeData.addUpperExclusive(max);
                }
            }
            default -> throw new RuntimeException("unknown list type " + listType);
        }

        return numRows;
    }

    @Override
    public WarpStoragePageSource.RowRanges reset(RangeData rangeData)
    {
        long[] lowerInclusive = rangeData.getLowerInclusiveAsArray();
        rangeData.clearLowerInclusive();
        long[] upperExclusive = rangeData.getUpperExclusiveAsArray();
        rangeData.clearUpperExclusive();
        return new WarpStoragePageSource.RowRanges(lowerInclusive, upperExclusive, false);
    }

    // in case merge was successful, removes the previous range and returns its min, otherwise return the input min
    private long mergeRanges(long min, RangeData rangeData)
    {
        if (rangeData.getUpperExclusiveSize() > 0 && rangeData.getUpperExclusiveValue(rangeData.getUpperExclusiveSize() - 1) == min) {
            min = rangeData.removeLowerInclusive(rangeData.getLowerInclusiveSize() - 1);
            rangeData.removeUpperExclusive(rangeData.getUpperExclusiveSize() - 1);
        }
        return min;
    }

    /**
     * inclusive min, exclusive max (etc: Range = [4-6], need to take rows 4,5)
     *
     * @return rangesCount - ranges in juffer
     */
    @Override
    public WarpStoragePageSource.RowRanges collectRanges(RangeData rangeData)
    {
        WarpStoragePageSource.RowRanges ranges = reset(rangeData);

        LongArrayList limitedLowerInclusive = new LongArrayList();
        LongArrayList limitedUpperExclusive = new LongArrayList();
        int currRange = 0;

        while (currRange < ranges.getRangesCount()) {
            int min = (int) ranges.getLowerInclusive(currRange);
            int max = (int) ranges.getUpperExclusive(currRange);
            limitedLowerInclusive.add(min);
            limitedUpperExclusive.add(max);
            currRange++;
        }

        return new WarpStoragePageSource.RowRanges(limitedLowerInclusive.toLongArray(), limitedUpperExclusive.toLongArray(), false);
    }
}
