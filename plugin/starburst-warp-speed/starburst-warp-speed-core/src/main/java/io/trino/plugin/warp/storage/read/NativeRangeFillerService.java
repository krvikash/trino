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
import io.airlift.log.Logger;
import io.trino.plugin.warp.gen.constants.RecordIndexListType;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

@Singleton
public class NativeRangeFillerService
        implements RangeFillerService
{
    private static final Logger logger = Logger.get(NativeRangeFillerService.class);

    @Inject
    public NativeRangeFillerService()
    {
    }

    // update start index. Return true if we have completed the chunk
    @Override
    public boolean updateStartIxIfNotCompleted(RangeData rangeData)
    {
        // if we have not started collecting the chunk we return false
        if (rangeData.getNumChunkRowsCollected() == 0) {
            return false;
        }

        int curStart = rangeData.getRecordIndexes().getStart() + rangeData.getNumChunkRowsCollected();
        if (rangeData.getRecordIndexes().getSize() == curStart) {
            return true;
        }
        rangeData.getRecordIndexes().setStart(curStart);
        return false;
    }

    // return the number of rows collected in this round
    @Override
    public int add(int chunkIndex, int numRows, QueryArgs queryArgs, AggregatorPageArgs aggregatorPageArgs, StorageCollectorService storageCollectorService)
    {
        RangeData rangeData = aggregatorPageArgs.rangeData();
        RecordIndexes recordIndexes = rangeData.getRecordIndexes();
        advanceChunkIfNeeded(chunkIndex, rangeData);

        // in case collected count is zero, it means nothing was collected regardless of the type
        if (numRows == 0) {
            return 0;
        }

        // if we are here we have at least one row that was collected
        RecordIndexListType listType = recordIndexes.getType();
        int baseRow = chunkIndex * queryArgs.chunkSize();
        boolean rangesRequired = queryArgs.queryParams().isRangesRequired();
        switch (listType) {
            case RECORD_INDEX_LIST_TYPE_FULL -> {
                numRows = queryArgs.chunkSize();
                if (rangesRequired) {
                    rangeData.addLowerInclusive(baseRow);
                    rangeData.addUpperExclusive(baseRow + numRows);
                }
            }
            case RECORD_INDEX_LIST_TYPE_ALL -> {
                if (rangesRequired) {
                    int min = baseRow + aggregatorPageArgs.rangeData().getRecordIndexes().getStart();
                    long minValue = mergeRanges(min, rangeData);
                    rangeData.addLowerInclusive(minValue);
                    rangeData.addUpperExclusive(min + numRows);
                }
            }
            case RECORD_INDEX_LIST_TYPE_VALUES -> {
                if (rangesRequired) {
                    // we take the rows from where we stopped last time
                    MemorySegment recordIndexesList = recordIndexes.getList();
                    int listIdx = rangeData.getNumChunkRowsCollected();

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

        // update number of rows collected from current chunk
        rangeData.incNumChunkRowsCollected(numRows);
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

    @Override
    public int getNumCollectedFromCurrentChunk(int chunkIndex, RangeData rangeData)
    {
        advanceChunkIfNeeded(chunkIndex, rangeData);
        return rangeData.getNumChunkRowsCollected();
    }

    // list type and size are kept as memebers
    // in type all we store the first row index in the byte array
    // in type all we store the part of the list we have not collected yet in the byte array
    @Override
    public StoreRowListResult storeRowList(ChunksQueue chunksQueue,
            QueryArgs queryArgs,
            AggregatorArgs aggregatorArgs,
            RangeData rangeData)
    {
        int currChunkIndex = chunksQueue.getCurrent();
        advanceChunkIfNeeded(currChunkIndex, rangeData);

        RecordIndexes recordIndexes = rangeData.getRecordIndexes();
        RecordIndexListType storeRowListType = recordIndexes.getType();
        byte[] storeRowListBuff = aggregatorArgs.storeRowListBuff();
        int storeRowListSize;
        Optional<Integer> storeRowListStart = Optional.empty(); // only for type ALL
        switch (storeRowListType) {
            case RECORD_INDEX_LIST_TYPE_FULL:
                storeRowListSize = queryArgs.chunkSize();
                break;
            case RECORD_INDEX_LIST_TYPE_ALL:
                storeRowListSize = rangeData.getRecordIndexes().getSize();
                storeRowListStart = Optional.of(recordIndexes.getStart());
                break;
            case RECORD_INDEX_LIST_TYPE_VALUES:
                storeRowListSize = rangeData.getRecordIndexes().getSize() - rangeData.getNumChunkRowsCollected();
                if (storeRowListSize == queryArgs.chunkSize()) {
                    storeRowListType = RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL;
                    break;
                }
                MemorySegment.copy(recordIndexes.getList(),
                        rangeData.getNumChunkRowsCollected() * ValueLayout.JAVA_SHORT.byteSize(),
                        MemorySegment.ofArray(storeRowListBuff),
                        0,
                        storeRowListSize * ValueLayout.JAVA_SHORT.byteSize());
                break;
            default:
                throw new RuntimeException("unknown list type " + storeRowListType);
        }
        logger.debug("storeRowList lastChunkIndex %d type %s size %d", rangeData.getLastChunkIndex(), storeRowListType, storeRowListSize);
        return new StoreRowListResult(storeRowListType, storeRowListSize, storeRowListStart, currChunkIndex);
    }

    @Override
    public void restoreRowList(RecordIndexes recordIndexes, StoreRowListResult storeRowListResult, byte[] storeRowListBuff)
    {
        RecordIndexListType storeRowListType = storeRowListResult.storeRowListType();
        recordIndexes.setType(storeRowListType);
        recordIndexes.setSize(storeRowListResult.storeRowListSize());
        switch (storeRowListType) {
            case RECORD_INDEX_LIST_TYPE_FULL:
                break;
            case RECORD_INDEX_LIST_TYPE_ALL:
                recordIndexes.setStart(storeRowListResult.storeRowListStart().get());
                break;
            case RECORD_INDEX_LIST_TYPE_VALUES:
                MemorySegment dstSegment = recordIndexes.getList();
                MemorySegment.copy(MemorySegment.ofArray(storeRowListBuff), 0, dstSegment, 0, storeRowListResult.storeRowListSize() * ValueLayout.JAVA_SHORT.byteSize());
                break;
            default:
                throw new RuntimeException("unknown list type " + storeRowListType);
        }
    }

    private void advanceChunkIfNeeded(int chunkIndex, RangeData rangeData)
    {
        if (rangeData.getLastChunkIndex() != chunkIndex) {
            rangeData.setLastChunkIndex(chunkIndex);
            rangeData.resetNumChunkRowsCollected();
        }
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
    public WarpStoragePageSource.RowRanges collectRanges(RangeData rangeData, int rowsLimit)
    {
        WarpStoragePageSource.RowRanges ranges = reset(rangeData);
        if (rowsLimit == Integer.MAX_VALUE) {
            return ranges;
        }

        LongArrayList limitedLowerInclusive = new LongArrayList();
        LongArrayList limitedUpperExclusive = new LongArrayList();
        int sum = 0;
        int currRange = 0;

        while (currRange < ranges.getRangesCount() && (sum < rowsLimit)) {
            int min = (int) ranges.getLowerInclusive(currRange);
            int max = (int) ranges.getUpperExclusive(currRange);
            sum += max - min;
            if (sum > rowsLimit) {
                max -= (sum - rowsLimit);
                sum = rowsLimit;
            }
            limitedLowerInclusive.add(min);
            limitedUpperExclusive.add(max);
            currRange++;
        }

        return new WarpStoragePageSource.RowRanges(limitedLowerInclusive.toLongArray(), limitedUpperExclusive.toLongArray(), false);
    }
}
