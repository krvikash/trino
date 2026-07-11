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
package io.trino.spi.block;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestBitArrayPreSizedBlockBuilder
{
    // positions: true, false, null
    private static final BitArrayBlock SOURCE = new BitArrayBlock(3, Optional.of(new long[] {0b011}), new long[] {0b001});

    @Test
    void testBuild()
    {
        // 70 entries so the block spans more than one 64-bit word
        int positionCount = 70;
        BitArrayPreSizedBlockBuilder builder = new BitArrayPreSizedBlockBuilder(positionCount);
        for (int i = 0; i < positionCount; i++) {
            switch (i % 6) {
                case 0 -> builder.writeBoolean(true);
                case 1 -> builder.writeBoolean(false);
                case 2 -> builder.appendNull();
                case 3 -> builder.append(SOURCE, 0);
                case 4 -> builder.append(SOURCE, 1);
                default -> builder.append(SOURCE, 2);
            }
        }

        Block block = builder.build();
        assertThat(block).isInstanceOf(BitArrayBlock.class);
        assertThat(block.getPositionCount()).isEqualTo(positionCount);
        BitArrayBlock bitArrayBlock = (BitArrayBlock) block;
        for (int i = 0; i < positionCount; i++) {
            switch (i % 6) {
                case 0, 3 -> {
                    assertThat(bitArrayBlock.isNull(i)).isFalse();
                    assertThat(bitArrayBlock.getBoolean(i)).isTrue();
                }
                case 1, 4 -> {
                    assertThat(bitArrayBlock.isNull(i)).isFalse();
                    assertThat(bitArrayBlock.getBoolean(i)).isFalse();
                }
                default -> assertThat(bitArrayBlock.isNull(i)).isTrue();
            }
        }
    }

    @Test
    void testNoNulls()
    {
        BitArrayPreSizedBlockBuilder builder = new BitArrayPreSizedBlockBuilder(2);
        builder.writeBoolean(false);
        builder.append(SOURCE, 0);

        BitArrayBlock block = (BitArrayBlock) builder.build();
        assertThat(block.mayHaveNull()).isFalse();
        assertThat(block.getBoolean(0)).isFalse();
        assertThat(block.getBoolean(1)).isTrue();
    }

    @Test
    void testAllNullsBuildsBitArrayBlock()
    {
        BitArrayPreSizedBlockBuilder builder = new BitArrayPreSizedBlockBuilder(3);
        builder.appendNull();
        builder.appendNull();
        builder.append(SOURCE, 2);

        Block block = builder.build();
        assertThat(block).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(block.getPositionCount()).isEqualTo(3);
        // the null value block must have the Boolean representation, or consumers casting to BitArrayBlock fail
        assertThat(((RunLengthEncodedBlock) block).getValue()).isInstanceOf(BitArrayBlock.class);
        assertThat(block.isNull(0)).isTrue();
    }

    @Test
    void testSingleNullBuildsBitArrayBlock()
    {
        BitArrayPreSizedBlockBuilder builder = new BitArrayPreSizedBlockBuilder(1);
        builder.appendNull();

        // RunLengthEncodedBlock.create returns the value block itself for a single position
        Block block = builder.build();
        assertThat(block.getPositionCount()).isEqualTo(1);
        assertThat(block.getUnderlyingValueBlock()).isInstanceOf(BitArrayBlock.class);
        assertThat(block.isNull(0)).isTrue();
    }

    @Test
    void testWritePastCapacityFails()
    {
        // 10 entries occupy a single 64-bit word, so the bitmaps alone would silently accept up to 64 writes
        BitArrayPreSizedBlockBuilder builder = new BitArrayPreSizedBlockBuilder(10);
        for (int i = 0; i < 10; i++) {
            builder.writeBoolean(i % 2 == 0);
        }

        assertThatThrownBy(() -> builder.writeBoolean(true)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(builder::appendNull).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> builder.append(SOURCE, 0)).isInstanceOf(IllegalStateException.class);

        assertThat(builder.build().getPositionCount()).isEqualTo(10);
    }

    @Test
    void testZeroCapacity()
    {
        BitArrayPreSizedBlockBuilder builder = new BitArrayPreSizedBlockBuilder(0);
        assertThatThrownBy(() -> builder.writeBoolean(true)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(builder::appendNull).isInstanceOf(IllegalStateException.class);
        assertThat(builder.build().getPositionCount()).isEqualTo(0);
    }
}
