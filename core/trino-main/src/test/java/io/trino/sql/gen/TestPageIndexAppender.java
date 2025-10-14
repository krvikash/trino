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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import io.trino.operator.PagesIndexAppender;
import io.trino.operator.SimplePageIndexAppender;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.block.BlockAssertions.assertBlockEquals;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createDoublesBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPageIndexAppender
{
    private static final OrderingCompiler ORDERING_COMPILER = new OrderingCompiler(new TypeOperators());

    @Test
    void testSingleChannel()
    {
        testAppender(ImmutableList.of(VARCHAR), ImmutableList.of(
                createStringsBlock("alice", "bob", "charlie", "dave")));

        testAppender(ImmutableList.of(BIGINT), ImmutableList.of(
                createLongsBlock(1, 2, 3, 4, 5)));

        testAppender(ImmutableList.of(DOUBLE), ImmutableList.of(
                createDoublesBlock(1.1, 2.2, 3.3, 4.4)));

        testAppender(ImmutableList.of(BOOLEAN), ImmutableList.of(
                createBooleansBlock(true, false, true, false)));
    }

    @Test
    void testMultipleChannels()
    {
        testAppender(
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                ImmutableList.of(
                        createStringsBlock("a", null, "c", "d", null, "f"),
                        createLongsBlock(1L, 2L, null, 4L, 5L, null),
                        createDoublesBlock(1.1, null, 3.3, null, 5.5, 6.6),
                        createBooleansBlock(true, false, null, true, null, false)));
    }

    @Test
    void testMultipleBlocks()
    {
        // Create multiple blocks in channels to simulate a real PagesIndex scenario
        ObjectArrayList<Block> varcharChannel = new ObjectArrayList<>();
        varcharChannel.add(createStringsBlock("a", "b", "c"));
        varcharChannel.add(createStringsBlock("d", "e", "f"));
        varcharChannel.add(createStringsBlock("g", "h", "i"));

        ObjectArrayList<Block> bigintChannel = new ObjectArrayList<>();
        bigintChannel.add(createLongsBlock(1, 2, 3));
        bigintChannel.add(createLongsBlock(4, 5, 6));
        bigintChannel.add(createLongsBlock(7, 8, 9));

        List<Type> types = ImmutableList.of(VARCHAR, BIGINT);
        @SuppressWarnings("unchecked")
        ObjectArrayList<Block>[] channels = new ObjectArrayList[] {varcharChannel, bigintChannel};

        PagesIndexAppender compiled = ORDERING_COMPILER.compilePagesIndexAppender(types, channels);
        PagesIndexAppender simple = new SimplePageIndexAppender(channels);

        // Test appending from different blocks at different positions
        PageBuilder compiledBuilder = new PageBuilder(types);
        PageBuilder simpleBuilder = new PageBuilder(types);

        // Append from first block, position 1
        compiledBuilder.declarePosition();
        compiled.append(0, 1, compiledBuilder);
        simpleBuilder.declarePosition();
        simple.append(0, 1, simpleBuilder);

        // Append from second block, position 2
        compiledBuilder.declarePosition();
        compiled.append(1, 2, compiledBuilder);
        simpleBuilder.declarePosition();
        simple.append(1, 2, simpleBuilder);

        // Append from third block, position 0
        compiledBuilder.declarePosition();
        compiled.append(2, 0, compiledBuilder);
        simpleBuilder.declarePosition();
        simple.append(2, 0, simpleBuilder);

        assertPagesEqual(types, compiledBuilder, simpleBuilder);
    }

    @Test
    void testEmptyBlocks()
    {
        testAppender(
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(
                        createStringsBlock(),
                        createLongsBlock(List.of())));
    }

    private void testAppender(List<Type> types, List<Block> blocks)
    {
        assertThat(types.size()).isEqualTo(blocks.size());

        // All blocks should have the same number of positions
        int positionCount = blocks.getFirst().getPositionCount();
        for (Block block : blocks) {
            assertThat(block.getPositionCount()).isEqualTo(positionCount);
        }

        // Create channels (single block per channel for simple test)
        @SuppressWarnings("unchecked")
        ObjectArrayList<Block>[] channels = new ObjectArrayList[types.size()];
        for (int i = 0; i < types.size(); i++) {
            channels[i] = new ObjectArrayList<>();
            channels[i].add(blocks.get(i));
        }

        // Create both compiled and simple appenders
        PagesIndexAppender compiled = ORDERING_COMPILER.compilePagesIndexAppender(types, channels);
        PagesIndexAppender simple = new SimplePageIndexAppender(channels);

        // Build pages using both appenders
        PageBuilder compiledBuilder = new PageBuilder(types);
        PageBuilder simpleBuilder = new PageBuilder(types);

        // Append all positions from block 0
        for (int position = 0; position < positionCount; position++) {
            compiledBuilder.declarePosition();
            compiled.append(0, position, compiledBuilder);

            simpleBuilder.declarePosition();
            simple.append(0, position, simpleBuilder);
        }

        // Verify that both builders produce identical pages
        assertPagesEqual(types, compiledBuilder, simpleBuilder);
    }

    private void assertPagesEqual(List<Type> types, PageBuilder compiledBuilder, PageBuilder simpleBuilder)
    {
        assertThat(compiledBuilder.getPositionCount()).isEqualTo(simpleBuilder.getPositionCount());

        for (int channel = 0; channel < types.size(); channel++) {
            Block compiledBlock = compiledBuilder.getBlockBuilder(channel).build();
            Block simpleBlock = simpleBuilder.getBlockBuilder(channel).build();

            assertBlockEquals(types.get(channel), compiledBlock, simpleBlock);
        }
    }
}
