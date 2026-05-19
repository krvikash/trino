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

import io.trino.plugin.warp.gen.stats.DispatcherPageSourceStats;
import io.trino.plugin.warp.gen.stats.NativeStats;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.min;

public record QueryArgs(
        QueryParams queryParams,
        DispatcherPageSourceStats dispatcherPageSourceStats,
        NativeStats nativeStats,
        long[] fileCookie,
        int chunkSize,
        int numChunks,
        int numChunksInRange,
        Optional<byte[]> storeMatchCollectMetadataBuff)
{
    int maxMatchedChunks()
    {
        return numChunksInRange;
    }

    int numRecordsInChunk(int chunkIx)
    {
        checkState(chunkIx < numChunks, "chunkIx %s is out of range", chunkIx);
        return min(queryParams.getTotalNumRecords(), chunkSize * (chunkIx + 1)) - (chunkSize * chunkIx);
    }
}
