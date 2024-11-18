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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.assertj.core.api.Assertions.assertThat;

public class CompressionStateTest
{
    private MemorySegment compressionStateMem;
    private MemorySegment compressionHits;
    private MemorySegment encodingHits;
    private CompressionState compressionState;

    @BeforeEach
    public void before()
    {
        compressionStateMem = Arena.ofAuto().allocate(CompressionState.COMPRESSION_STATE_LAYOUT.byteSize(), ValueLayout.JAVA_INT.byteSize());
        compressionHits = compressionStateMem.asSlice(CompressionState.COMPRESSION_STATE_OFFSET_COMPRESSION_HITS, CompressionState.COMPRESSION_HITS_LAYOUT);
        encodingHits = compressionStateMem.asSlice(CompressionState.COMPRESSION_STATE_OFFSET_ENCODING_HITS, CompressionState.ENCODING_HITS_LAYOUT);
        compressionState = new CompressionState(compressionStateMem);
    }

    @Test
    public void testChooseCompression()
    {
        compressionState.reset();
        compressionHits.setAtIndex(ValueLayout.JAVA_INT, CompressionAlg.COMPRESSION_ALG_NONE.ordinal(), 70);
        compressionHits.setAtIndex(ValueLayout.JAVA_INT, CompressionAlg.COMPRESSION_ALG_LZ4.ordinal(), 98);
        compressionHits.setAtIndex(ValueLayout.JAVA_INT, CompressionAlg.COMPRESSION_ALG_LZ4HC.ordinal(), 100);
        encodingHits.setAtIndex(ValueLayout.JAVA_INT, EncodingAlg.ENCODING_ALG_NONE.ordinal(), 73);
        encodingHits.setAtIndex(ValueLayout.JAVA_INT, EncodingAlg.ENCODING_ALG_BIT_PACKING.ordinal(), 99);
        encodingHits.setAtIndex(ValueLayout.JAVA_INT, EncodingAlg.ENCODING_ALG_BIT_PACKING_DELTA.ordinal(), 80);

        compressionState.setAlgorithm(CompressionState.DECISION_POINT_NUM_CHUNKS + 1);
        assertThat(CompressionAlg.values()[compressionStateMem.get(ValueLayout.JAVA_BYTE, CompressionState.COMPRESSION_STATE_OFFSET_BEST_COMPRESSION_ALG)]).isEqualTo(CompressionAlg.COMPRESSION_ALG_LZ4HC);
        assertThat(EncodingAlg.values()[compressionStateMem.get(ValueLayout.JAVA_BYTE, CompressionState.COMPRESSION_STATE_OFFSET_BEST_ENCODING_ALG)]).isEqualTo(EncodingAlg.ENCODING_ALG_NONE);
    }

    @Test
    public void testChooseEncoding()
    {
        compressionState.reset();
        compressionHits.setAtIndex(ValueLayout.JAVA_INT, CompressionAlg.COMPRESSION_ALG_NONE.ordinal(), 70);
        compressionHits.setAtIndex(ValueLayout.JAVA_INT, CompressionAlg.COMPRESSION_ALG_LZ4.ordinal(), 99);
        compressionHits.setAtIndex(ValueLayout.JAVA_INT, CompressionAlg.COMPRESSION_ALG_LZ4HC.ordinal(), 98);
        encodingHits.setAtIndex(ValueLayout.JAVA_INT, EncodingAlg.ENCODING_ALG_NONE.ordinal(), 73);
        encodingHits.setAtIndex(ValueLayout.JAVA_INT, EncodingAlg.ENCODING_ALG_BIT_PACKING.ordinal(), 100);
        encodingHits.setAtIndex(ValueLayout.JAVA_INT, EncodingAlg.ENCODING_ALG_BIT_PACKING_DELTA.ordinal(), 80);

        compressionState.setAlgorithm(CompressionState.DECISION_POINT_NUM_CHUNKS + 2);
        assertThat(CompressionAlg.values()[compressionStateMem.get(ValueLayout.JAVA_BYTE, CompressionState.COMPRESSION_STATE_OFFSET_BEST_COMPRESSION_ALG)]).isEqualTo(CompressionAlg.COMPRESSION_ALG_NONE);
        assertThat(EncodingAlg.values()[compressionStateMem.get(ValueLayout.JAVA_BYTE, CompressionState.COMPRESSION_STATE_OFFSET_BEST_ENCODING_ALG)]).isEqualTo(EncodingAlg.ENCODING_ALG_BIT_PACKING);
    }

    @Test
    public void testFailedCompressionAndEncdoging()
    {
        compressionState.reset();

        compressionHits.setAtIndex(ValueLayout.JAVA_INT, CompressionAlg.COMPRESSION_ALG_NONE.ordinal(), 103);
        compressionHits.setAtIndex(ValueLayout.JAVA_INT, CompressionAlg.COMPRESSION_ALG_LZ4.ordinal(), 20);
        compressionHits.setAtIndex(ValueLayout.JAVA_INT, CompressionAlg.COMPRESSION_ALG_LZ4HC.ordinal(), 10);
        encodingHits.setAtIndex(ValueLayout.JAVA_INT, EncodingAlg.ENCODING_ALG_NONE.ordinal(), 117);
        encodingHits.setAtIndex(ValueLayout.JAVA_INT, EncodingAlg.ENCODING_ALG_BIT_PACKING.ordinal(), 34);
        encodingHits.setAtIndex(ValueLayout.JAVA_INT, EncodingAlg.ENCODING_ALG_BIT_PACKING_DELTA.ordinal(), 57);
        compressionState.setAlgorithm(CompressionState.CYCLE_SIZE_NUM_CHUNKS - 1);
        assertThat(CompressionAlg.values()[compressionStateMem.get(ValueLayout.JAVA_BYTE, CompressionState.COMPRESSION_STATE_OFFSET_BEST_COMPRESSION_ALG)]).isEqualTo(CompressionAlg.COMPRESSION_ALG_NONE);
        assertThat(EncodingAlg.values()[compressionStateMem.get(ValueLayout.JAVA_BYTE, CompressionState.COMPRESSION_STATE_OFFSET_BEST_ENCODING_ALG)]).isEqualTo(EncodingAlg.ENCODING_ALG_NONE);

        compressionState.setAlgorithm(CompressionState.CYCLE_SIZE_NUM_CHUNKS);
        assertThat(CompressionAlg.values()[compressionStateMem.get(ValueLayout.JAVA_BYTE, CompressionState.COMPRESSION_STATE_OFFSET_BEST_COMPRESSION_ALG)]).isEqualTo(CompressionAlg.COMPRESSION_ALG_UNKNOWN);
        assertThat(EncodingAlg.values()[compressionStateMem.get(ValueLayout.JAVA_BYTE, CompressionState.COMPRESSION_STATE_OFFSET_BEST_ENCODING_ALG)]).isEqualTo(EncodingAlg.ENCODING_ALG_UNKNOWN);
    }
}
