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

@Singleton
public class ChunksQueueService
{
    @Inject
    public ChunksQueueService()
    {
    }

    int getChunkIndexForMatch(ChunksQueue chunksQueue)
    {
        return chunksQueue.getTotalNumChunks();
    }

    void updateChunkRangeAfterMatch(ChunksQueue chunksQueue, int endChunkIndex, int numMatchedChunks, short[] matchedChunksIndexes, int[] matchBitmapResetPoints)
    {
        chunksQueue.add(endChunkIndex, numMatchedChunks, matchedChunksIndexes, matchBitmapResetPoints);
    }

    // return true if completely finished, false otherwise
    boolean updateChunkRangeFullScan(ChunksQueue chunksQueue, int numChunks, int numChunksInRange)
    {
        if (isCompletelyFinished(chunksQueue, numChunks)) {
            return true;
        }
        int startChunkIndex = chunksQueue.getTotalNumChunks();
        chunksQueue.add(startChunkIndex, Math.min(startChunkIndex + numChunksInRange, numChunks));
        return false;
    }

    boolean isChunkRangeCompleted(ChunksQueue chunksQueue)
    {
        return chunksQueue.isEmpty();
    }

    boolean storeRestoreRequired(ChunksQueue chunksQueue)
    {
        return !isChunkRangeCompleted(chunksQueue);
    }

    boolean isCompletelyFinished(ChunksQueue chunksQueue, int numChunks)
    {
        return isChunkRangeCompleted(chunksQueue) && (chunksQueue.getTotalNumChunks() >= numChunks);
    }

    boolean isChunkPreparationNeeded(ChunksQueue chunksQueue)
    {
        return !chunksQueue.isFirstChunkPrepared();
    }

    void setFirstChunkPrepared(ChunksQueue chunksQueue)
    {
        chunksQueue.setFirstChunkPrepared(true);
    }
}
