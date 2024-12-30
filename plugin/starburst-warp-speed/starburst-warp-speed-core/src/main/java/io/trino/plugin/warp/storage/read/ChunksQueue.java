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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.warp.storage.read.MatchState.MATCH_BITMAP_DESC_OFFSET_RESET_POINT;

public class ChunksQueue
{
    private final int allSet;
    private final int pageSize;
    private final int maxChunks;
    private final Deque<MatchChunkState> chunksToCollect;

    private int totalNumChunks; // in case queue is empty we return the total number of chunks
    private boolean firstChunkPrepared;
    private Optional<List<MemorySegment>> rootBitmapsDescriptors;
    private Optional<MemorySegment> rootBitmaps;

    ChunksQueue(int maxChunks, int chunkSize, int pageSize)
    {
        this.maxChunks = maxChunks;
        this.allSet = chunkSize; // constant used for setting full bitmaps in full scan case
        this.pageSize = pageSize;
        this.chunksToCollect = new ArrayDeque<>(maxChunks);
        initRootBitmaps();
    }

    // return if there is at least one chunk with matched records
    boolean anyMatchedChunkExists(int numChunksInRange)
    {
        List<MemorySegment> bitmapsDescriptors = rootBitmapsDescriptors.get().subList(0, numChunksInRange);
        for (MemorySegment bitmapDescriptor : bitmapsDescriptors) {
            int bitmapResetPoint = (int) bitmapDescriptor.get(ValueLayout.JAVA_INT, MATCH_BITMAP_DESC_OFFSET_RESET_POINT);
            if (bitmapResetPoint > 0) {
                return true;
            }
        }
        totalNumChunks += numChunksInRange; // in case there is no matched we advance the total counter
        return false;
    }

    // add more chunks to collect and update total number of chunks
    void updateChunkRangeAfterMatch(int numChunksInRange)
    {
        List<MemorySegment> bitmapsDescriptors = rootBitmapsDescriptors.get().subList(0, numChunksInRange);
        for (MemorySegment bitmapDescriptor : bitmapsDescriptors) {
            int bitmapResetPoint = (int) bitmapDescriptor.get(ValueLayout.JAVA_INT, MATCH_BITMAP_DESC_OFFSET_RESET_POINT);
            if (bitmapResetPoint > 0) {
                chunksToCollect.add(new MatchChunkState(totalNumChunks, Optional.of(bitmapDescriptor)));
            }
            totalNumChunks++;
        }
    }

    // return true if completely finished, false otherwise
    boolean updateChunkRangeFullScan(int numChunks, int numChunksInRange)
    {
        if (isCompletelyFinished(numChunks)) {
            return true;
        }
        int startChunkIndex = totalNumChunks;
        int endChunkIndex = Math.min(startChunkIndex + numChunksInRange, numChunks);
        for (int chunkIndex = startChunkIndex; chunkIndex < endChunkIndex; chunkIndex++) {
            chunksToCollect.add(new MatchChunkState(chunkIndex, Optional.empty()));
        }
        this.totalNumChunks = endChunkIndex;
        return false;
    }

    // get the current chunk to collect
    int getCurrent()
    {
        return chunksToCollect.getFirst().getChunkIndex();
    }

    // get the current chunk match bitmap reset point
    int getCurrentBitmapResetPoint()
    {
        int bitmapResetPoint;

        // lazy restore
        MatchChunkState matchChunkState = chunksToCollect.getFirst();
        if (matchChunkState.shouldRestore()) {
            final int chunkIndex = matchChunkState.getChunkIndex();
            Optional<MemorySegment> bitmapDescriptor = rootBitmapsDescriptors.map(l -> Optional.of(l.get(calcMatchBitmapIndex(chunkIndex)))).orElse(Optional.empty());
            bitmapResetPoint = matchChunkState.restored(bitmapDescriptor);
            bitmapDescriptor.ifPresent(bm -> bm.set(ValueLayout.JAVA_INT, MATCH_BITMAP_DESC_OFFSET_RESET_POINT, bitmapResetPoint));
            if (bitmapResetPoint > allSet) {
                final int offsetInBuff = calcMatchBitmapOffset(chunkIndex);
                MemorySegment.copy(MemorySegment.ofArray(matchChunkState.getBitmapBuffer()), 0, rootBitmaps.get(), offsetInBuff, pageSize);
            }
            return bitmapResetPoint;
        }
        return matchChunkState.getBitmapDescriptor().map(bm -> bm.get(ValueLayout.JAVA_INT, MATCH_BITMAP_DESC_OFFSET_RESET_POINT)).orElse(allSet);
    }

    private Optional<MemorySegment> getBmOfChunkIx(int chunkIndex)
    {
        int bmOffset = calcMatchBitmapOffset(chunkIndex);
        return rootBitmaps.map(memorySegment -> memorySegment.asSlice(bmOffset, pageSize));
    }

    int prepareCurRecList(RecordIndexes recordIndexes, int bmResetPoint)
    {
        int numRecInBm = bmResetPoint;
        if (bmResetPoint > allSet) {
            // bm must be valid since bmResetPoint is not
            MemorySegment bm = getBmOfChunkIx(getCurrent()).get();
            numRecInBm = recordIndexes.setRecIxListFromBM(bm);
        }
        return numRecInBm;
    }

    void storeMatchBitmaps()
    {
        if (chunksToCollect.isEmpty()) {
            return;
        }

        MemorySegment rootBitmapsMem = rootBitmaps.orElse(null); // will be used only if the descriptor is valid and the reset point is invalid
        Iterator<MatchChunkState> itr = chunksToCollect.iterator();
        while (itr.hasNext()) {
            MatchChunkState matchChunkState = itr.next();
            if (matchChunkState.shouldStore()) {
                int bitmapResetPoint = (int) matchChunkState.getBitmapDescriptor().map(bm -> bm.get(ValueLayout.JAVA_INT, MATCH_BITMAP_DESC_OFFSET_RESET_POINT)).orElse(allSet);
                Optional<byte[]> bitmapBufferOpt = Optional.empty();
                // value larger than allSet means the reset point is not valid and the bitmap is used
                if (bitmapResetPoint > allSet) {
                    final int offsetInBuff = calcMatchBitmapOffset(matchChunkState.getChunkIndex());
                    byte[] bitmapBuffer = new byte[pageSize];
                    MemorySegment.copy(rootBitmapsMem, offsetInBuff, MemorySegment.ofArray(bitmapBuffer), 0, pageSize);
                    bitmapBufferOpt = Optional.of(bitmapBuffer);
                }
                matchChunkState.stored(bitmapResetPoint, bitmapBufferOpt);
            }
        }
    }

    private int calcMatchBitmapIndex(int chunkIndex)
    {
        return chunkIndex & (maxChunks - 1);
    }

    // we assume that numChunksInRange is a power of 2
    private int calcMatchBitmapOffset(int chunkIndex)
    {
        return calcMatchBitmapIndex(chunkIndex) * pageSize;
    }

    // advance to the next chunk to collect
    void currentCompleted()
    {
        chunksToCollect.poll();
        setIsFirstChunkPrepared(false);
    }

    public void setIsFirstChunkPrepared(boolean firstChunkPrepared)
    {
        this.firstChunkPrepared = firstChunkPrepared;
    }

    public boolean isFirstChunkPrepared()
    {
        return firstChunkPrepared;
    }

    int getChunkIndexForMatch()
    {
        return totalNumChunks;
    }

    boolean isChunkRangeCompleted()
    {
        return chunksToCollect.isEmpty();
    }

    boolean isCompletelyFinished(int numChunks)
    {
        return isChunkRangeCompleted() && (totalNumChunks >= numChunks);
    }

    boolean isChunkPreparationNeeded()
    {
        return !isFirstChunkPrepared();
    }

    void setFirstChunkAsPrepared()
    {
        setIsFirstChunkPrepared(true);
    }

    void setRootBitmaps(Optional<MemorySegment> rootBitmaps, List<MemorySegment> rootBitmapsDescriptors)
    {
        this.rootBitmaps = rootBitmaps;
        this.rootBitmapsDescriptors = Optional.of(rootBitmapsDescriptors);
    }

    void initRootBitmaps()
    {
        this.rootBitmaps = Optional.empty();
        this.rootBitmapsDescriptors = Optional.empty();
    }

    private static class MatchChunkState
    {
        private int chunkIndex;
        // state
        boolean isLoaded;
        private Optional<MemorySegment> bitmapDescriptor;
        // store
        private int storeBitmapResetPoint;
        private Optional<byte[]> storeBitmapBuffer;

        MatchChunkState(int chunkIndex, Optional<MemorySegment> bitmapDescriptor)
        {
            this.chunkIndex = chunkIndex;
            this.bitmapDescriptor = bitmapDescriptor;
            this.isLoaded = true;
        }

        int getChunkIndex()
        {
            return chunkIndex;
        }

        Optional<MemorySegment> getBitmapDescriptor()
        {
            return bitmapDescriptor;
        }

        byte[] getBitmapBuffer()
        {
            return storeBitmapBuffer.get();
        }

        void stored(int bitmapResetPoint, Optional<byte[]> bitmapBuffer)
        {
            this.storeBitmapResetPoint = bitmapResetPoint;
            this.storeBitmapBuffer = bitmapBuffer;
            this.bitmapDescriptor = Optional.empty();
            this.isLoaded = false;
        }

        int restored(Optional<MemorySegment> bitmapDescriptor)
        {
            this.bitmapDescriptor = bitmapDescriptor;
            this.isLoaded = true;
            return this.storeBitmapResetPoint;
        }

        boolean shouldStore()
        {
            return isLoaded;
        }

        boolean shouldRestore()
        {
            return !isLoaded;
        }
    }
}
