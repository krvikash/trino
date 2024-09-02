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
package io.trino.plugin.warp.storage.engine;

import io.trino.plugin.warp.storage.read.StorageCollectorCallBack;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StubsStorageEngine
        implements StorageEngine
{
    private final List<RuntimeException> throwOnColletRuntimeExceptionList = new ArrayList<>();
    ByteBuffer firstBundle;

    public StubsStorageEngine()
    {
    }

    @Override
    public void initRecordBufferSizes(int[] fixedRecordBufferSizes, int[] varlenRecordBufferSizes)
    {
        for (int i = 0; i < fixedRecordBufferSizes.length; i++) {
            fixedRecordBufferSizes[i] = 64 * 1024 * i;
        }
        for (int i = 0; i < varlenRecordBufferSizes.length; i++) {
            varlenRecordBufferSizes[i] = 64 * 1024 * i;
        }
    }

    @Override
    public void initCollectTxSizes(int[] fixedCollectTxSizes, int[] varlenCollectTxSizes)
    {
        for (int i = 0; i < fixedCollectTxSizes.length; i++) {
            fixedCollectTxSizes[i] = 1024 * i;
        }
        for (int i = 0; i < varlenCollectTxSizes.length; i++) {
            varlenCollectTxSizes[i] = 1024 * i;
        }
    }

    @Override
    public long initWarmupTxSizes(int[] fixedWarmupDataTxSizes, int[] varlenWarmupDataTxSizes)
    {
        for (int i = 0; i < fixedWarmupDataTxSizes.length; i++) {
            fixedWarmupDataTxSizes[i] = 1024 * i;
        }
        for (int i = 0; i < varlenWarmupDataTxSizes.length; i++) {
            varlenWarmupDataTxSizes[i] = 1024 * i;
        }
        return 1024;
    }

    @Override
    public int fileOpen(String fileName)
    {
        return 0;
    }

    @Override
    public void fileClose(int fileDescriptor)
    {
    }

    @Override
    public void fileTruncate(int fileDescriptor, int offset)
    {
    }

    @Override
    public void filePunchHole(String fileName, int startOffset, int endOffset)
    {
    }

    @Override
    public void fileIsAboutToBeDeleted(long fileHash, long fileModTime, int fileSizeInPages)
    {
    }

    @Override
    public long warmupElementOpen(long context, int recTypeCode, int recTypeLength, int warmUpType)
    {
        return 1;
    }

    @Override
    public long warmupElementClose(int recTypeCode,
            int recTypeLength,
            int warmUpType,
            int numChunks,
            long[] fileCookie,
            long[] buffAddresses,
            int[] outQueryFileParams)
    {
        outQueryFileParams[0] = 0;
        outQueryFileParams[1] = 1;
        return 1;
    }

    @Override
    public void warmupVerifyQueryOffset(int queryOffset, long[] fileCookie)
    {
    }

    @Override
    public long warmupChunk(long weCookie, int addedNumRows, int addedNV, int addedBytes, long valueMin, long valueMax, int singleValOffset,
            boolean close, int recTypeCode, int recTypeLength, int warmUpType, long[] fileCookieParams, long[] buffAddresses,
            byte[] inOutCompressionStats, byte[] inOutChunkHeader, int[] outWarmEvents)
    {
        return 0;
    }

    @Override
    public long warmupChunkExtRec(long weCookie, int extRecordFirstOffset, int addedExtBytes, int recTypeCode, int recTypeLength, int warmUpType,
            long[] fileCookieParams, long[] buffAddresses, byte[] inOutChunkHeader)
    {
        return 0;
    }

    @Override
    public int queryGetCollect2MatchSize()
    {
        return 0;
    }

    @Override
    public long queryGetCollectStateSize(int numMatchCollect)
    {
        return 0;
    }

    @Override
    public long collectOpen(int totalNumRecords, long[] fileCookie, byte[] parsingBuff, byte[] collect2MatchParams, int numCollectWes,
            int numChunksInRange, int[] weCollectParams, int connectorId, long matchBitmapAddress, int minOffset,
            long[][] outCollectColBuffIds, long[] outMetadataBuffIds)
    {
        Arrays.fill(outMetadataBuffIds, 0);
        Arrays.fill(firstBundle.array(), (byte) 0);
        firstBundle.position(0);
        return 0;
    }

    @Override
    public long matchOpen(int totalNumRecords, long[] fileCookie, int collectTxId, byte[] parsingBuff, byte[] collect2MatchParams, int numMatchWes,
            int numChunksInRange, int[] weMatchTree, long matchBitmapAddress, int minOffset, long[][] outMatchColBuffIds)
    {
        return 0;
    }

    @Override
    public long collectRestoreState(int txId, int chunkIndex, StorageCollectorCallBack collectStateObj)
    {
        return 0;
    }

    @Override
    public long matchAgg(int txId, int startChunkIndex)
    {
        return 0L;
    }

    @Override
    public long matchLucenePrepare(int matchTxId, int matchWeIx, int startChunkIndex, int numChunks, long[] outParams)
    {
        return 0L;
    }

    @Override
    public void matchLuceneCompleted(int matchTxId, int matchWeIx, int startChunkIndex, int numChunks, int[] matchResult)
    {
    }

    @Override
    public long match(int txId, int startChunkIndex, int numChunks, short[] outMatchedChunksIndexes, int[] outMatchBitmapResetPoints)
    {
        return 0L;
    }

    @Override
    public long processMatchResult(int txId, int chunkIndex, int bitmapResetPoint, int rowsLimit)
    {
        return 0;
    }

    @Override
    public long processFullScanChunk(int txId, int chunkIndex, int startRowIx, int rowsLimit)
    {
        return 0;
    }

    @Override
    public void collect(int txId, int numWes, int chunkIndex, int numToCollect, int[] outResultTypes)
    {
        if (!throwOnColletRuntimeExceptionList.isEmpty()) {
            throw throwOnColletRuntimeExceptionList.removeFirst();
        }
    }

    @Override
    public long collectClose(int txId, int[] chunksWithBitmapsToStore, int numChunksWithBitmaps, StorageCollectorCallBack obj)
    {
        return 0;
    }

    @Override
    public void matchClose(int txId)
    {
    }

    @Override
    public ByteBuffer getBundleFromPool(int bufIx)
    {
        if (bufIx == 0) {
            firstBundle = ByteBuffer.allocate(1 << 20);
            return firstBundle;
        }
        return ByteBuffer.allocate(1 << 20);
    }

    @Override
    public void setDebugThrowPolicy(int numElements, int[] panicID, int[] repetitionMode, int[] ratio)
    {
    }

    @Override
    public String executeDebugCommand(String commandName, int numParams, String[] paramNames, String[] paramValues)
    {
        return "";
    }

    public synchronized void clear()
    {
        throwOnColletRuntimeExceptionList.clear();
    }

    public void setThrowOnCollect(RuntimeException... e)
    {
        throwOnColletRuntimeExceptionList.addAll(Arrays.asList(e));
    }
}
