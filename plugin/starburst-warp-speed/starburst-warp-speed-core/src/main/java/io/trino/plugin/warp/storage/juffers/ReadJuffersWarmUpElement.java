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
package io.trino.plugin.warp.storage.juffers;

import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.juffer.BufferAllocator;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.util.Optional;

public class ReadJuffersWarmUpElement
        extends JuffersWarmUpElementBase
{
    Optional<LuceneBMResultJuffer> luceneBMResultJuffer;

    // for empty element with no buffers (basic index case)
    public ReadJuffersWarmUpElement()
    {
        super();
        luceneBMResultJuffer = Optional.empty();
    }

    // can be collect or lucene
    public ReadJuffersWarmUpElement(BufferAllocator bufferAllocator, boolean isCollect)
    {
        super();

        if (isCollect) {
            RecordReadJuffer recordJuffers = new RecordReadJuffer(bufferAllocator);
            juffers.put(recordJuffers.getJufferType(), recordJuffers);

            NullReadJuffer nullJuffers = new NullReadJuffer(bufferAllocator);
            juffers.put(nullJuffers.getJufferType(), nullJuffers);
        }
        else {
            luceneBMResultJuffer = Optional.of(new LuceneBMResultJuffer(bufferAllocator));
        }
    }

    public void createBuffers(RecTypeCode recTypeCode, int recTypeLength, MemorySegment[] buffs)
    {
        for (BaseJuffer juffer : juffers.values()) {
            BaseReadJuffer readJuffer = (BaseReadJuffer) juffer;
            readJuffer.createBuffer(recTypeCode, recTypeLength, buffs);
        }
    }

    public void createLuceneBuffers(MemorySegment luceneBitmaps, int luceneBitmapOffset)
    {
        if (luceneBMResultJuffer.isPresent() && (luceneBitmaps != null)) {
            luceneBMResultJuffer.get().createLuceneBuffer(luceneBitmaps, luceneBitmapOffset);
        }
    }

    public ByteBuffer getLuceneBMResultBuffer()
    {
        return (ByteBuffer) luceneBMResultJuffer.get().getWrappedBuffer();
    }
}
