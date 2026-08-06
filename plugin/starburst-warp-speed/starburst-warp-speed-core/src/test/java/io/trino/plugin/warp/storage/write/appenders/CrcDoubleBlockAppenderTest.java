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

import io.trino.plugin.warp.storage.write.WarmupElementStats;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.LongBuffer;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

class CrcDoubleBlockAppenderTest
        extends BlockAppenderTest
{
    @Override
    @BeforeEach
    public void beforeEach()
    {
        super.beforeEach();
        blockAppender = new CrcDoubleBlockAppender(writeJuffersWarmUpElement);
    }

    static Stream<Arguments> params()
    {
        DoubleType doubleType = DoubleType.DOUBLE;
        BlockBuilder blockBuilder = doubleType.createBlockBuilder(null, 5);
        List<Double> values = List.of(-1.5, 2.7, 3.9, 4.2, 5.1);
        for (double val : values) {
            doubleType.writeDouble(blockBuilder, val);
        }
        double expectedMaxValue = values.stream()
                .max(Double::compare)
                .orElse(null);
        double expectedMinValue = values.stream()
                .min(Double::compare)
                .orElse(null);
        Block blockWithoutNull = blockBuilder.build();
        blockBuilder.appendNull();
        Block blockWithNull = blockBuilder.build();
        return Stream.of(
                arguments(blockWithoutNull, DoubleType.DOUBLE, new WarmupElementStats(0, expectedMinValue, expectedMaxValue)),
                arguments(blockWithNull, DoubleType.DOUBLE, new WarmupElementStats(1, expectedMinValue, expectedMaxValue)),
                arguments(
                        DictionaryBlock.create(5, blockWithNull, new int[] {5, 0, 4, 1, 5}),
                        DoubleType.DOUBLE,
                        new WarmupElementStats(2, -1.5, 5.1)),
                arguments(
                        RunLengthEncodedBlock.create(new LongArrayBlock(1, Optional.empty(), new long[] {Double.doubleToLongBits(3.5)}), 4),
                        DoubleType.DOUBLE,
                        new WarmupElementStats(0, 3.5, 3.5)));
    }

    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void write(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(LongBuffer.allocate(100));
        runTest(block, blockType, expectedResult);
    }
}
