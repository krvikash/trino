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

import io.airlift.slice.Slice;
import io.trino.plugin.warp.juffer.BufferAllocator;

import java.lang.foreign.MemorySegment;

/**
 * buffer for marking extended strings
 */
public class LuceneWriteJuffer
        extends BaseWriteJuffer
{
    private static final int SINGLE_UNINITIALIZED = -1;
    private static final int SINGLE_DISABLED = 0;
    private static final int SINGLE_ENABLED = 1;

    private int luceneBufferSingleExist;
    private Slice luceneBufferSingleSlice;

    public LuceneWriteJuffer(BufferAllocator bufferAllocator)
    {
        super(bufferAllocator, JuffersType.LUCENE);
    }

    @Override
    public JuffersType getJufferType()
    {
        return JuffersType.LUCENE;
    }

    @Override
    public void createBuffer(MemorySegment[] buffs)
    {
        luceneBufferSingleExist = SINGLE_UNINITIALIZED;
    }

    public void updateLuceneProps(Slice val)
    {
        if (luceneBufferSingleExist == SINGLE_DISABLED) {
            return; // do nothing we already know its not single value
        }
        if (luceneBufferSingleExist == SINGLE_UNINITIALIZED) {
            // first value of the chunk
            luceneBufferSingleExist = SINGLE_ENABLED;
            luceneBufferSingleSlice = val;
        }
        else {
            if (!val.equals(luceneBufferSingleSlice)) {
                luceneBufferSingleExist = SINGLE_DISABLED; // non single value
            }
        }
    }

    public boolean getSingleAndResetLuceneWE()
    {
        boolean isSingleValue = (luceneBufferSingleExist == SINGLE_ENABLED);
        luceneBufferSingleExist = SINGLE_UNINITIALIZED;
        return isSingleValue;
    }
}
