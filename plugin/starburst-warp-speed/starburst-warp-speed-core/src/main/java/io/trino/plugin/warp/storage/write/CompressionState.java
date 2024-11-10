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
package io.trino.plugin.warp.storage.write;

import io.trino.plugin.warp.gen.constants.CompressionAlg;
import io.trino.plugin.warp.gen.constants.EncodingAlg;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;

public class CompressionState
{
    public static final StructLayout COMPRESSION_STATE_LAYOUT;
    private static final SequenceLayout COMPRESSION_HITS_LAYOUT;
    private static final SequenceLayout ENCODING_HITS_LAYOUT;
    //private static final long COMPRESSION_STATE_OFFSET_COMPRESSION_HITS;
    //private static final long COMPRESSION_STATE_OFFSET_ENCODING_HITS;
    //private static final long COMPRESSION_STATE_OFFSET_COMPRESSION_ALG_IN;
    //private static final long COMPRESSION_STATE_OFFSET_ENCODING_ALG_IN;
    //private static final long COMPRESSION_STATE_OFFSET_UPDATE_COUNTER;

    private final MemorySegment compressionState;

    static {
        COMPRESSION_HITS_LAYOUT = MemoryLayout.sequenceLayout(CompressionAlg.COMPRESSION_ALG_NUM_OF.ordinal(), ValueLayout.JAVA_INT);
        ENCODING_HITS_LAYOUT = MemoryLayout.sequenceLayout(EncodingAlg.ENCODING_ALG_NUM_OF.ordinal(), ValueLayout.JAVA_INT);

        COMPRESSION_STATE_LAYOUT = MemoryLayout.structLayout(
                COMPRESSION_HITS_LAYOUT.withName("compression_hits"),
                ENCODING_HITS_LAYOUT.withName("encoding_hits"),
                ValueLayout.JAVA_BYTE.withName("compression_alg_in"),
                ValueLayout.JAVA_BYTE.withName("encoding_alg_in"),
                ValueLayout.JAVA_BYTE.withName("update_counter")).withName("compression_state_t");
        //COMPRESSION_STATE_OFFSET_COMPRESSION_HITS = COMPRESSION_STATE_LAYOUT.byteOffset(PathElement.groupElement("compression_hits"));
        //COMPRESSION_STATE_OFFSET_ENCODING_HITS = COMPRESSION_STATE_LAYOUT.byteOffset(PathElement.groupElement("encoding_hits"));
        //COMPRESSION_STATE_OFFSET_COMPRESSION_ALG_IN = COMPRESSION_STATE_LAYOUT.byteOffset(PathElement.groupElement("compression_alg_in"));
        //COMPRESSION_STATE_OFFSET_ENCODING_ALG_IN = COMPRESSION_STATE_LAYOUT.byteOffset(PathElement.groupElement("encoding_alg_in"));
        //COMPRESSION_STATE_OFFSET_UPDATE_COUNTER = COMPRESSION_STATE_LAYOUT.byteOffset(PathElement.groupElement("update_counter"));
    }

    public CompressionState(MemorySegment compressionState)
    {
        this.compressionState = compressionState;
    }

    public long getAddress()
    {
        return compressionState.address();
    }

    // storage engine assumes the state is zeroed when starting to warm a new element
    public void reset()
    {
        compressionState.fill((byte) 0);
    }
}
