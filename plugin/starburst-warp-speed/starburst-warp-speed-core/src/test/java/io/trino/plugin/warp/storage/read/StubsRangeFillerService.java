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

import com.google.inject.Singleton;

@Singleton
public class StubsRangeFillerService
        implements RangeFillerService
{
    public StubsRangeFillerService() {}

    // return the number of rows collected in this round
    @Override
    public int add(RecordIndexes recordIndexes, ChunkProperties chunkProperties, QueryArgs queryArgs, RangeData rangeData)
    {
        return 0;
    }

    @Override
    public WarpStoragePageSource.RowRanges collectRanges(RangeData rangeData)
    {
        return WarpStoragePageSource.RowRanges.EMPTY;
    }
}
