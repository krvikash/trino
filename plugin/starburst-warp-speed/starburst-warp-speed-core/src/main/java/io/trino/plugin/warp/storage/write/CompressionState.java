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

import io.airlift.log.Logger;
import io.trino.plugin.warp.gen.constants.CompressionAlg;
import io.trino.plugin.warp.gen.constants.EncodingAlg;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemoryLayout.PathElement;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.StructLayout;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class CompressionState
{
    private static final Logger logger = Logger.get(CompressionState.class);
    // Number of new chunks before a decision can be applied to cmprs algorithm
    static final byte DECISION_POINT_NUM_CHUNKS = 4;
    // Number of new chunks before statistics are reset
    static final byte CYCLE_SIZE_NUM_CHUNKS = 8;

    public static final StructLayout COMPRESSION_STATE_LAYOUT;
    static final SequenceLayout COMPRESSION_HITS_LAYOUT;
    static final SequenceLayout ENCODING_HITS_LAYOUT;
    static final long COMPRESSION_STATE_OFFSET_COMPRESSION_HITS;
    static final long COMPRESSION_STATE_OFFSET_ENCODING_HITS;
    static final long COMPRESSION_STATE_OFFSET_BEST_COMPRESSION_ALG;
    static final long COMPRESSION_STATE_OFFSET_BEST_ENCODING_ALG;

    private final MemorySegment compressionState;

    static {
        COMPRESSION_HITS_LAYOUT = MemoryLayout.sequenceLayout(CompressionAlg.COMPRESSION_ALG_NUM_OF.ordinal(), ValueLayout.JAVA_INT);
        ENCODING_HITS_LAYOUT = MemoryLayout.sequenceLayout(EncodingAlg.ENCODING_ALG_NUM_OF.ordinal(), ValueLayout.JAVA_INT);

        COMPRESSION_STATE_LAYOUT = MemoryLayout.structLayout(
                COMPRESSION_HITS_LAYOUT.withName("compression_hits"),
                ENCODING_HITS_LAYOUT.withName("encoding_hits"),
                ValueLayout.JAVA_BYTE.withName("best_compression_alg"),
                ValueLayout.JAVA_BYTE.withName("best_encoding_alg")).withName("compression_state_t");
        COMPRESSION_STATE_OFFSET_COMPRESSION_HITS = COMPRESSION_STATE_LAYOUT.byteOffset(PathElement.groupElement("compression_hits"));
        COMPRESSION_STATE_OFFSET_ENCODING_HITS = COMPRESSION_STATE_LAYOUT.byteOffset(PathElement.groupElement("encoding_hits"));
        COMPRESSION_STATE_OFFSET_BEST_COMPRESSION_ALG = COMPRESSION_STATE_LAYOUT.byteOffset(PathElement.groupElement("best_compression_alg"));
        COMPRESSION_STATE_OFFSET_BEST_ENCODING_ALG = COMPRESSION_STATE_LAYOUT.byteOffset(PathElement.groupElement("best_encoding_alg"));
    }

    public CompressionState(MemorySegment compressionState)
    {
        this.compressionState = compressionState;
    }

    public MemorySegment getMemory()
    {
        return compressionState;
    }

    // storage engine assumes the state is zeroed when starting to warm a new element
    public void reset()
    {
        compressionState.fill((byte) 0);
    }

    public void setAlgorithm(int numChunks)
    {
        // consider statistical information to decide on compression/encoding algorithm if we reached the decision point
        if (isDecideThresholdReached(numChunks) &&
                (getCompressionAlg() == CompressionAlg.COMPRESSION_ALG_UNKNOWN) &&
                (getEncodingAlg() == EncodingAlg.ENCODING_ALG_UNKNOWN)) {
            BestCompressionAlg bestCompressionAlg = findBestCompressionAlg();
            BestEncodingAlg bestEncodingAlg = findBestEncodingAlg();

            // choose the best of the best
            if (bestEncodingAlg.hits() > bestCompressionAlg.hits()) {
                setEncodingAlg(bestEncodingAlg.alg());
                setCompressionAlg(CompressionAlg.COMPRESSION_ALG_NONE);
                logger.debug("bestEncodingAlg %s", bestEncodingAlg);
            }
            else {
                setCompressionAlg(bestCompressionAlg.alg());
                setEncodingAlg(EncodingAlg.ENCODING_ALG_NONE);
                logger.debug("bestCompressionAlg %s", bestCompressionAlg);
            }
        }

        // after decision is taken check if we need to reset in case reset point reached and no algorithm chosen so far
        if (isResetThresholdReached(numChunks) &&
                (getCompressionAlg() == CompressionAlg.COMPRESSION_ALG_NONE) &&
                (getEncodingAlg() == EncodingAlg.ENCODING_ALG_NONE)) {
            // it means we failed to compress/encode. since we reached the reset threshold we reset the state and start over
            reset();
        }
    }

    private BestCompressionAlg findBestCompressionAlg()
    {
        int bestCompressionHits = -1;
        MemorySegment compressionHits = getCompressionHits();
        CompressionAlg bestCompressionAlg = CompressionAlg.COMPRESSION_ALG_UNKNOWN;
        Set<CompressionAlg> compressionAlgCandidates = Arrays.stream(CompressionAlg.values())
                .filter(compressionAlg -> !CompressionAlg.COMPRESSION_ALG_UNKNOWN.equals(compressionAlg) && !CompressionAlg.COMPRESSION_ALG_NUM_OF.equals(compressionAlg))
                .collect(Collectors.toSet());
        for (CompressionAlg compressionAlg : compressionAlgCandidates) {
            int currHits = getCompressionAlgHit(compressionHits, compressionAlg);
            if (currHits > bestCompressionHits) {
                bestCompressionHits = currHits;
                bestCompressionAlg = compressionAlg;
            }
        }
        return new BestCompressionAlg(bestCompressionAlg, bestCompressionHits);
    }

    private BestEncodingAlg findBestEncodingAlg()
    {
        int bestEncodingHits = -1;
        MemorySegment encodingHits = getEncodingHits();
        EncodingAlg bestEncodingAlg = EncodingAlg.ENCODING_ALG_UNKNOWN;
        Set<EncodingAlg> encodingAlgCandidates = Arrays.stream(EncodingAlg.values())
                .filter(encodingAlg -> !EncodingAlg.ENCODING_ALG_UNKNOWN.equals(encodingAlg) && !EncodingAlg.ENCODING_ALG_NUM_OF.equals(encodingAlg))
                .collect(Collectors.toSet());
        for (EncodingAlg encodingAlg : encodingAlgCandidates) {
            int currHits = getEncodingAlgHit(encodingHits, encodingAlg);
            if (currHits > bestEncodingHits) {
                bestEncodingHits = currHits;
                bestEncodingAlg = encodingAlg;
            }
        }
        return new BestEncodingAlg(bestEncodingAlg, bestEncodingHits);
    }

    private boolean isDecideThresholdReached(int numChunks)
    {
        return ((numChunks % CYCLE_SIZE_NUM_CHUNKS) > DECISION_POINT_NUM_CHUNKS);
    }

    private boolean isResetThresholdReached(int numChunks)
    {
        return ((numChunks % CYCLE_SIZE_NUM_CHUNKS) == 0);
    }

    private CompressionAlg getCompressionAlg()
    {
        return CompressionAlg.values()[compressionState.get(ValueLayout.JAVA_BYTE, COMPRESSION_STATE_OFFSET_BEST_COMPRESSION_ALG)];
    }

    private void setCompressionAlg(CompressionAlg compressionAlg)
    {
        compressionState.set(ValueLayout.JAVA_BYTE, COMPRESSION_STATE_OFFSET_BEST_COMPRESSION_ALG, (byte) compressionAlg.ordinal());
    }

    private EncodingAlg getEncodingAlg()
    {
        return EncodingAlg.values()[compressionState.get(ValueLayout.JAVA_BYTE, COMPRESSION_STATE_OFFSET_BEST_ENCODING_ALG)];
    }

    private void setEncodingAlg(EncodingAlg encodingAlg)
    {
        compressionState.set(ValueLayout.JAVA_BYTE, COMPRESSION_STATE_OFFSET_BEST_ENCODING_ALG, (byte) encodingAlg.ordinal());
    }

    private MemorySegment getCompressionHits()
    {
        return compressionState.asSlice(COMPRESSION_STATE_OFFSET_COMPRESSION_HITS, COMPRESSION_HITS_LAYOUT);
    }

    private int getCompressionAlgHit(MemorySegment compressionHits, CompressionAlg compressionAlg)
    {
        return compressionHits.getAtIndex(ValueLayout.JAVA_INT, compressionAlg.ordinal());
    }

    private MemorySegment getEncodingHits()
    {
        return compressionState.asSlice(COMPRESSION_STATE_OFFSET_ENCODING_HITS, ENCODING_HITS_LAYOUT);
    }

    private int getEncodingAlgHit(MemorySegment encodingHits, EncodingAlg encodingAlg)
    {
        return encodingHits.getAtIndex(ValueLayout.JAVA_INT, encodingAlg.ordinal());
    }

    private record BestCompressionAlg(CompressionAlg alg, int hits)
    {
        @Override
        public String toString()
        {
            return "BestCompressionAlg{alg=" + alg + " ,hits=" + hits + "}";
        }
    }

    private record BestEncodingAlg(EncodingAlg alg, int hits)
    {
        @Override
        public String toString()
        {
            return "BestEncodingAlg{alg=" + alg + " ,hits=" + hits + "}";
        }
    }
}
