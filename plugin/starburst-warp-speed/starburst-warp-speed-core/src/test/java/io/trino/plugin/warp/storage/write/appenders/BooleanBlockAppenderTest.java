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
package io.trino.plugin.warp.storage.write.appenders;

import io.trino.plugin.warp.juffer.BlockPosHolder;
import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.plugin.warp.storage.write.WarmupElementStatsBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.ByteArrayBlock;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

class BooleanBlockAppenderTest
        extends BlockAppenderTest
{
    private ByteBuffer recordBuffer;

    @Override
    @BeforeEach
    public void beforeEach()
    {
        super.beforeEach();
        blockAppender = new BooleanBlockAppender(writeJuffersWarmUpElement);
        recordBuffer = ByteBuffer.allocate(100);
        when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(recordBuffer);
    }

    static Stream<Arguments> params()
    {
        Type blockType = BooleanType.BOOLEAN;
        ByteArrayBlock valuesWithNull = new ByteArrayBlock(3, Optional.of(new boolean[] {false, true, false}), new byte[] {1, 0, 0});
        return Stream.of(
                arguments(new ByteArrayBlock(3, Optional.empty(), new byte[] {1, 0, 1}),
                        blockType,
                        new WarmupElementStats(false, 0, null, null, false)),
                arguments(
                        valuesWithNull,
                        blockType,
                        new WarmupElementStats(false, 1, null, null, false)),
                arguments(DictionaryBlock.create(4, valuesWithNull, new int[] {0, 1, 2, 1}),
                        blockType,
                        new WarmupElementStats(false, 2, null, null, false)),
                arguments(RunLengthEncodedBlock.create(new ByteArrayBlock(1, Optional.empty(), new byte[] {1}), 5),
                        blockType,
                        new WarmupElementStats(false, 0, null, null, false)),
                arguments(RunLengthEncodedBlock.create(new ByteArrayBlock(1, Optional.of(new boolean[] {true}), new byte[] {0}), 5),
                        blockType,
                        new WarmupElementStats(false, 5, null, null, false)));
    }

    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void write(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        runTest(block, blockType, expectedResult);
    }

    static Stream<Arguments> nonZeroValueParams()
    {
        ByteArrayBlock values = new ByteArrayBlock(2, Optional.empty(), new byte[] {2, 0});
        return Stream.of(
                arguments(values, new byte[] {1, 0}),
                arguments(DictionaryBlock.create(3, values, new int[] {0, 1, 0}), new byte[] {1, 0, 1}),
                arguments(RunLengthEncodedBlock.create(new ByteArrayBlock(1, Optional.empty(), new byte[] {2}), 3), new byte[] {1, 1, 1}));
    }

    @ParameterizedTest
    @MethodSource("nonZeroValueParams")
    void writeNormalizesNonZeroValues(Block block, byte[] expectedRecords)
    {
        BlockPosHolder blockPosHolder = new BlockPosHolder(block, BooleanType.BOOLEAN, 0, block.getPositionCount());
        blockAppender.append(jufferPos, blockPosHolder, null, new WarmupElementStatsBuilder());

        byte[] records = new byte[expectedRecords.length];
        recordBuffer.rewind().get(records);
        assertThat(records).isEqualTo(expectedRecords);
    }
}
