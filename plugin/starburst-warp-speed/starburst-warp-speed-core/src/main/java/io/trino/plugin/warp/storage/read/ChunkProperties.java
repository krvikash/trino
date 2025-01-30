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

import java.util.Objects;

public final class ChunkProperties
{
    private final int chunkIndex;
    private int numRecordsInChunk;
    private final RecordIndexListType type;
    private int startIx;

    public ChunkProperties(int chunkIndex, int numRecordsInChunk, RecordIndexListType type, int startIx)
    {
        this.chunkIndex = chunkIndex;
        this.numRecordsInChunk = numRecordsInChunk;
        this.type = type;
        this.startIx = startIx;
    }

    public int chunkIndex()
    {
        return chunkIndex;
    }

    public int numRecordsInChunk()
    {
        return numRecordsInChunk;
    }

    public RecordIndexListType type()
    {
        return type;
    }

    public int startIx()
    {
        return startIx;
    }

    public void reduceNumRecordsInChunk(int numToReduce)
    {
        numRecordsInChunk -= numToReduce;
    }

    public void setStartIx(int startIx)
    {
        this.startIx = startIx;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (ChunkProperties) obj;
        return this.chunkIndex == that.chunkIndex &&
                this.numRecordsInChunk == that.numRecordsInChunk &&
                Objects.equals(this.type, that.type) &&
                this.startIx == that.startIx;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(chunkIndex, numRecordsInChunk, type, startIx);
    }

    @Override
    public String toString()
    {
        return "ChunkProperties[" +
                "chunkIndex=" + chunkIndex + ", " +
                "numRecordsInChunk=" + numRecordsInChunk + ", " +
                "type=" + type + ", " +
                "startIx=" + startIx + ']';
    }
}
