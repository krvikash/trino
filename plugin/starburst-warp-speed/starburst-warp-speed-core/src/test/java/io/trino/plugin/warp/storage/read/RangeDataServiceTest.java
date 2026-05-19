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

import io.trino.plugin.warp.storage.memory.ThreadArena;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.plugin.warp.gen.constants.FileCookieParams.FILE_COOKIE_PARAMS_NUM_OF;
import static io.trino.plugin.warp.gen.constants.RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL;
import static io.trino.plugin.warp.gen.constants.RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RangeDataServiceTest
{
    private RangeFillerService rangeFillerService;
    private QueryArgs queryArgs;
    private MemorySegment recordIndexesList;
    private RecordIndexes recordIndexes;
    private AtomicLong numAllocatedBytes;

    @BeforeEach
    public void before()
    {
        numAllocatedBytes = new AtomicLong();
        recordIndexes = new RecordIndexes((int) Math.pow(2, 16));
        recordIndexes.allocateRecordIndexesSegment(new ThreadArena(this::onClose, numAllocatedBytes, null));
        MemorySegment recordIndexesMem = recordIndexes.getRecordIndexesSegment();
        recordIndexesList = recordIndexesMem.asSlice(RecordIndexes.RECORD_INDEXES_OFFSET_LIST, RecordIndexes.RECORD_INDEXES_LIST_LAYOUT);

        queryArgs = mock(QueryArgs.class);

        QueryParams queryParams = mock(QueryParams.class);
        when(queryParams.isRangesRequired()).thenReturn(true);
        when(queryParams.getTotalNumRecords()).thenReturn(80);
        when(queryParams.getNumCollectElements()).thenReturn(1);
        when(queryParams.getCollectElementsParamsList()).thenReturn(Collections.emptyList());
        when(queryArgs.fileCookie()).thenReturn(new long[FILE_COOKIE_PARAMS_NUM_OF.ordinal()]);
        when(queryArgs.queryParams()).thenReturn(queryParams);
        rangeFillerService = new NativeRangeFillerService();
    }

    public void onClose() {}

    @Test
    public void testAllOneShot()
    {
        RangeData rangeData = new RangeData();

        when(queryArgs.chunkSize()).thenReturn(1);
        ChunkProperties chunkProperties = new ChunkProperties(0, 5, RECORD_INDEX_LIST_TYPE_ALL, 3);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);
        WarpStoragePageSource.RowRanges ranges = rangeFillerService.collectRanges(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(1);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(8);
    }

    @Test
    public void testAllTwoShots()
    {
        RangeData rangeData = new RangeData();

        when(queryArgs.chunkSize()).thenReturn(1);
        ChunkProperties chunkProperties = new ChunkProperties(0, 5, RECORD_INDEX_LIST_TYPE_ALL, 3);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        chunkProperties = new ChunkProperties(0, 10, RECORD_INDEX_LIST_TYPE_ALL, 8);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        WarpStoragePageSource.RowRanges ranges = rangeFillerService.collectRanges(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(1);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(18);
    }

    @Test
    public void testValuesOneShot()
    {
        RangeData rangeData = new RangeData();

        // 1st range [3-6)
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 0, (short) 3);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 1, (short) 4);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 2, (short) 5);
        // 2nd range [30-31)
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 3, (short) 30);
        // 3rd range [49-54)
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 4, (short) 49);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 5, (short) 50);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 6, (short) 51);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 7, (short) 52);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 8, (short) 53);
        // 4th range [63-64)
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 9, (short) 63);

        when(queryArgs.chunkSize()).thenReturn(1);

        ChunkProperties chunkProperties = new ChunkProperties(0, 10, RECORD_INDEX_LIST_TYPE_VALUES, 0);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        WarpStoragePageSource.RowRanges ranges = rangeFillerService.collectRanges(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(4);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(6);
        assertThat(ranges.getLowerInclusive(1)).isEqualTo(30);
        assertThat(ranges.getUpperExclusive(1)).isEqualTo(31);
        assertThat(ranges.getLowerInclusive(2)).isEqualTo(49);
        assertThat(ranges.getUpperExclusive(2)).isEqualTo(54);
        assertThat(ranges.getLowerInclusive(3)).isEqualTo(63);
        assertThat(ranges.getUpperExclusive(3)).isEqualTo(64);
    }

    @Test
    public void testValuesTwoShots()
    {
        RangeData rangeData = new RangeData();

        // 1st range [3-6)
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 0, (short) 3);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 1, (short) 4);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 2, (short) 5);
        // 2nd range [30-31)
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 3, (short) 30);
        // 3rd range [49-54)
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 4, (short) 49);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 5, (short) 50);

        when(queryArgs.chunkSize()).thenReturn(1);

        ChunkProperties chunkProperties = new ChunkProperties(0, 6, RECORD_INDEX_LIST_TYPE_VALUES, 0);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 6, (short) 51);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 7, (short) 52);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 8, (short) 53);
        // 4th range [63-64)
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 9, (short) 63);
        chunkProperties = new ChunkProperties(0, 4, RECORD_INDEX_LIST_TYPE_VALUES, 6);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        WarpStoragePageSource.RowRanges ranges = rangeFillerService.collectRanges(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(4);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(6);
        assertThat(ranges.getLowerInclusive(1)).isEqualTo(30);
        assertThat(ranges.getUpperExclusive(1)).isEqualTo(31);
        assertThat(ranges.getLowerInclusive(2)).isEqualTo(49);
        assertThat(ranges.getUpperExclusive(2)).isEqualTo(54);
        assertThat(ranges.getLowerInclusive(3)).isEqualTo(63);
        assertThat(ranges.getUpperExclusive(3)).isEqualTo(64);
    }

    @Test
    public void testAllAndThenValues()
    {
        RangeData rangeData = new RangeData();

        when(queryArgs.chunkSize()).thenReturn(1);
        ChunkProperties chunkProperties = new ChunkProperties(0, 5, RECORD_INDEX_LIST_TYPE_ALL, 3);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        // start in record list from index 0
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 0, (short) 8);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 1, (short) 9);
        chunkProperties = new ChunkProperties(0, 2, RECORD_INDEX_LIST_TYPE_VALUES, 0); // start ix in the recordList is 0
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 2, (short) 13);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 3, (short) 14);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 4, (short) 15);
        chunkProperties = new ChunkProperties(0, 3, RECORD_INDEX_LIST_TYPE_VALUES, 2);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        WarpStoragePageSource.RowRanges ranges = rangeFillerService.collectRanges(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(2);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(3);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(10);
        assertThat(ranges.getLowerInclusive(1)).isEqualTo(13);
        assertThat(ranges.getUpperExclusive(1)).isEqualTo(16);
    }

    @Test
    public void testValuesAndThenAllAndThenValues()
    {
        RangeData rangeData = new RangeData();

        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 0, (short) 25);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 1, (short) 26);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 2, (short) 27);

        when(queryArgs.chunkSize()).thenReturn(1);
        ChunkProperties chunkProperties = new ChunkProperties(0, 3, RECORD_INDEX_LIST_TYPE_VALUES, 0);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        chunkProperties = new ChunkProperties(0, 22, RECORD_INDEX_LIST_TYPE_ALL, 28);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 3, (short) 50);
        chunkProperties = new ChunkProperties(0, 1, RECORD_INDEX_LIST_TYPE_VALUES, 3);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);
        recordIndexesList.setAtIndex(ValueLayout.JAVA_SHORT, 4, (short) 60);
        chunkProperties = new ChunkProperties(0, 1, RECORD_INDEX_LIST_TYPE_VALUES, 4);
        rangeFillerService.add(recordIndexes, chunkProperties, queryArgs, rangeData);

        WarpStoragePageSource.RowRanges ranges = rangeFillerService.collectRanges(rangeData);
        assertThat(ranges.getRangesCount()).isEqualTo(2);
        assertThat(ranges.getLowerInclusive(0)).isEqualTo(25);
        assertThat(ranges.getUpperExclusive(0)).isEqualTo(51);
        assertThat(ranges.getLowerInclusive(1)).isEqualTo(60);
        assertThat(ranges.getUpperExclusive(1)).isEqualTo(61);
    }
}
