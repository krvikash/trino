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

public interface RangeFillerService
{
    // check if the current chunk is done
    boolean updateStartIxIfNotCompleted(RangeData rangeData);

    // return the number of rows collected in this round
    int add(ChunkProperties chunkProperties, QueryArgs queryArgs, AggregatorPageArgs aggregatorPageArgs, StorageCollectorService storageCollectorService);

    WarpStoragePageSource.RowRanges reset(RangeData rangeData);

    WarpStoragePageSource.RowRanges collectRanges(RangeData rangeData, int rowsLimit);
}
