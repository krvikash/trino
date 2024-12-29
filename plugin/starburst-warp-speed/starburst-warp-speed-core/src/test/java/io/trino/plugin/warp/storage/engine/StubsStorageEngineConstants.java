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

public class StubsStorageEngineConstants
        implements StorageEngineConstants
{
    private final int varcharMaxLen;

    public StubsStorageEngineConstants()
    {
        this(1024);
    }

    public StubsStorageEngineConstants(int varcharMaxLen)
    {
        this.varcharMaxLen = varcharMaxLen;
    }

    @Override
    public int getMaxRecLen()
    {
        return 64;
    }

    @Override
    public int getPageOffsetMask()
    {
        return 8191;
    }

    @Override
    public int getPageSize()
    {
        return 8192;
    }

    @Override
    public int getPageSizeMask()
    {
        return ~8191;
    }

    @Override
    public int getPageSizeShift()
    {
        return 13;
    }

    @Override
    public int getRecordBufferMaxSize()
    {
        return 1 << 15;
    }

    @Override
    public int getIndexChunkMaxSize()
    {
        return 1024 * 1024;
    }

    @Override
    public int getQueryStringNullValueSize()
    {
        return 2;
    }

    @Override
    public int getChunksBufferMaxSize()
    {
        return 1 << 19;
    }

    @Override
    public int getChunkHeaderMaxSize()
    {
        return 32;
    }

    @Override
    public int getWarmupDataTempBufferSize()
    {
        return 1024 * 1024;
    }

    @Override
    public int getWarmupIndexTempBufferSize()
    {
        return 1024 * 1024;
    }

    @Override
    public int getMaxWeContextSize()
    {
        return 16 * 1024;
    }

    @Override
    public int getFixedLengthStringLimit()
    {
        return 8;
    }

    @Override
    public int getVarcharMaxLen()
    {
        return varcharMaxLen;
    }

    @Override
    public int getVarlenExtLimit()
    {
        return 61;
    }

    @Override
    public int getVarlenExtMark()
    {
        return 63;
    }

    @Override
    public int getVarlenExtRecordHeaderSize()
    {
        return 55;
    }

    @Override
    public int getVarlenMarkEnd()
    {
        return 62;
    }

    @Override
    public int getVarlenMdGranularity()
    {
        return 32;
    }

    @Override
    public int getChunkSizeShift()
    {
        return 13;
    }

    @Override
    public int getMatchCollectBufferSize()
    {
        return 1024 * 1024;
    }

    @Override
    public int getMatchCollectMetadataSize()
    {
        return 8;
    }

    @Override
    public int getMaxChunksInRange()
    {
        return 1;
    }

    @Override
    public int getMatchCollectNumIds()
    {
        return 1;
    }

    @Override
    public int getMaxMatchColumns()
    {
        return 8;
    }

    @Override
    public int getMatchTxSize()
    {
        return 16 * 1024;
    }

    @Override
    public int getMatchStatePayload()
    {
        return 32;
    }

    @Override
    public int getCollectStatePayload()
    {
        return 32;
    }
}
