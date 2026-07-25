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
import io.trino.spi.block.ShortArrayBlock;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ShortBuffer;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.when;

class CrcSmallIntBlockAppenderTest
        extends BlockAppenderTest
{
    @Override
    @BeforeEach
    public void beforeEach()
    {
        super.beforeEach();
        blockAppender = new CrcSmallIntBlockAppender(writeJuffersWarmUpElement);
    }

    static Stream<Arguments> params()
    {
        Type blockType = SmallintType.SMALLINT;
        return Stream.of(
                arguments(new ShortArrayBlock(3, Optional.empty(), new short[] {1, 2, 3}),
                        blockType,
                        new WarmupElementStats(0, (short) 1, (short) 3)),
                arguments(new ShortArrayBlock(4, Optional.of(new boolean[] {false, false, false, true}), new short[] {1, -50, 30, 333}),
                        blockType,
                        new WarmupElementStats(1, (short) -50, (short) 30)));
    }

    @Override
    @ParameterizedTest
    @MethodSource("params")
    public void writeWithoutDictionary(Block block, Type blockType, WarmupElementStats expectedResult)
    {
        when(writeJuffersWarmUpElement.getRecordBuffer()).thenReturn(ShortBuffer.allocate(100));
        runTest(block, blockType, expectedResult);
    }
}
