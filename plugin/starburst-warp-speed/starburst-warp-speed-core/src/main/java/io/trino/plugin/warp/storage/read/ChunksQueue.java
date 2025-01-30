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

import io.trino.plugin.warp.gen.constants.RecordIndexListType;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.warp.gen.constants.RecordIndexListType.RECORD_INDEX_LIST_TYPE_ALL;
import static io.trino.plugin.warp.gen.constants.RecordIndexListType.RECORD_INDEX_LIST_TYPE_VALUES;
import static io.trino.plugin.warp.storage.read.MatchState.MATCH_BITMAP_DESC_OFFSET_RESET_POINT;

public class ChunksQueue
{
    private final int allSet;
    private final int pageSize;
    private final int maxChunks;
    private final Deque<MatchChunkState> matchedChunks;
    private Optional<ChunkProperties> loadedChunkOpt;

    private int totalNumChunks; // in case queue is empty we return the total number of chunks
    private Optional<List<MemorySegment>> rootBitmapsDescriptors;
    private Optional<MemorySegment> rootBitmaps;

    ChunksQueue(int maxChunks, int chunkSize, int pageSize)
    {
        this.maxChunks = maxChunks;
        this.allSet = chunkSize; // constant used for setting full bitmaps in full scan case
        this.pageSize = pageSize;
        this.matchedChunks = new ArrayDeque<>(maxChunks);
        loadedChunkOpt = Optional.empty();
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
                matchedChunks.add(new MatchChunkState(totalNumChunks, Optional.of(bitmapDescriptor)));
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
            matchedChunks.add(new MatchChunkState(chunkIndex, Optional.empty()));
        }
        this.totalNumChunks = endChunkIndex;
        return false;
    }

    // get the current chunk to collect
    int getCurrent()
    {
        return loadedChunkOpt.map(ChunkProperties::chunkIndex).orElseGet(() -> matchedChunks.getFirst().getChunkIndex());
    }

    boolean isResetPointValid(int resetPoint)
    {
        return resetPoint <= allSet;
    }

    ChunkProperties loadChunk(RecordIndexes recordIndexes, int numRecordsInCurChunk)
    {
        if (loadedChunkOpt.isPresent()) {
            ChunkProperties chunk = loadedChunkOpt.get();
            if (chunk.type() == RECORD_INDEX_LIST_TYPE_VALUES) {
                chunk.setStartIx(0);
            }
            loadedChunkOpt = Optional.empty();
            return chunk;
        }

        int chunkIndex = getCurrent();
        int numRecordsInChunk;
        RecordIndexListType type;
        int startIx = 0;

        int bmResetPoint = getCurrentBitmapResetPoint(numRecordsInCurChunk);
        if (!isResetPointValid(bmResetPoint)) {
            // bm must be valid since bmResetPoint is not
            MemorySegment bm = getBmOfChunkIx(chunkIndex).get();
            numRecordsInChunk = recordIndexes.setRecIxListFromBM(bm);
            if (numRecordsInChunk == numRecordsInCurChunk) {
                type = RECORD_INDEX_LIST_TYPE_ALL;
            }
            else {
                type = RECORD_INDEX_LIST_TYPE_VALUES;
                startIx = recordIndexes.getCurOffset() - numRecordsInChunk;
            }
        }
        else {
            numRecordsInChunk = bmResetPoint;
            type = RECORD_INDEX_LIST_TYPE_ALL;
        }
        currentCompleted();
        return new ChunkProperties(chunkIndex, numRecordsInChunk, type, startIx);
    }

    // get the current chunk match bitmap reset point
    int getCurrentBitmapResetPoint(int numRecordsInCurChunk)
    {
        int bitmapResetPoint;

        // lazy restore
        MatchChunkState matchChunkState = matchedChunks.getFirst();
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
        return matchChunkState.getBitmapDescriptor().map(bm -> bm.get(ValueLayout.JAVA_INT, MATCH_BITMAP_DESC_OFFSET_RESET_POINT)).orElse(numRecordsInCurChunk);
    }

    private Optional<MemorySegment> getBmOfChunkIx(int chunkIndex)
    {
        int bmOffset = calcMatchBitmapOffset(chunkIndex);
        return rootBitmaps.map(memorySegment -> memorySegment.asSlice(bmOffset, pageSize));
    }

    void storeMatchBitmaps(QueryArgs queryArgs)
    {
        if (matchedChunks.isEmpty()) {
            return;
        }

        MemorySegment rootBitmapsMem = rootBitmaps.orElse(null); // will be used only if the descriptor is valid and the reset point is invalid
        Iterator<MatchChunkState> itr = matchedChunks.iterator();
        while (itr.hasNext()) {
            MatchChunkState matchChunkState = itr.next();
            if (matchChunkState.shouldStore()) {
                int bitmapResetPoint = matchChunkState.getBitmapDescriptor().map(bm ->
                        bm.get(ValueLayout.JAVA_INT, MATCH_BITMAP_DESC_OFFSET_RESET_POINT)).orElse(queryArgs.numRecordsInChunk(matchChunkState.chunkIndex));
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
        matchedChunks.poll();
    }

    int getChunkIndexForMatch()
    {
        return totalNumChunks;
    }

    boolean matchedChunksCompleted()
    {
        return matchedChunks.isEmpty() && !hasStoredChunk();
    }

    private boolean hasStoredChunk()
    {
        return loadedChunkOpt.isPresent();
    }

    boolean isCompletelyFinished(int numChunks)
    {
        return matchedChunksCompleted() && (totalNumChunks >= numChunks);
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

    public void returnLoadedChunk(ChunkProperties loadedChunk)
    {
        loadedChunkOpt = Optional.of(loadedChunk);
    }

    public Optional<ChunkProperties> getOptLoadedChunkProperties()
    {
        return loadedChunkOpt;
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
