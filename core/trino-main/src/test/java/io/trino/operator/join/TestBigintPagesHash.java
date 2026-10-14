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
package io.trino.operator.join;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.operator.JoinPagesIndex;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.operator.join.JoinUtils.getSingleBigintJoinChannel;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigintPagesHash
{
    @Test
    public void testBigintKeys()
    {
        assertJoinPositions(BIGINT);
    }

    @Test
    public void testIntegerKeys()
    {
        assertJoinPositions(INTEGER);
    }

    @Test
    public void testDateKeys()
    {
        assertJoinPositions(DATE);
    }

    @Test
    public void testSmallintKeys()
    {
        assertJoinPositions(SMALLINT);
    }

    @Test
    public void testTinyintKeys()
    {
        assertJoinPositions(TINYINT);
    }

    @Test
    public void testSingleBigintJoinChannelTypes()
    {
        for (Type type : ImmutableList.of(BIGINT, INTEGER, DATE, SMALLINT, TINYINT)) {
            assertThat(getSingleBigintJoinChannel(ImmutableList.of(1), ImmutableList.of(VARCHAR, type))).hasValue(1);
        }
        for (Type type : ImmutableList.of(REAL, DOUBLE, VARCHAR, BOOLEAN, TIMESTAMP_MILLIS)) {
            assertThat(getSingleBigintJoinChannel(ImmutableList.of(1), ImmutableList.of(VARCHAR, type))).isEmpty();
        }
        assertThat(getSingleBigintJoinChannel(ImmutableList.of(0, 1), ImmutableList.of(INTEGER, INTEGER))).isEmpty();
    }

    private static void assertJoinPositions(Type type)
    {
        long min = (Long) type.getRange().orElseThrow().getMin();
        long max = (Long) type.getRange().orElseThrow().getMax();
        List<Page> buildPages = ImmutableList.of(
                new Page(valueBlock(type, -100L, null, 0L, 5L, 5L, 120L)),
                new Page(valueBlock(type, 1L, 7L, null, 7L, -1L).getRegion(1, 3)),
                new Page(DictionaryBlock.create(5, valueBlock(type, 7L, null, -100L), new int[] {2, 0, 1, 2, 0}).getRegion(1, 4)),
                new Page(RunLengthEncodedBlock.create(type, 42L, 3)),
                new Page(RunLengthEncodedBlock.create(type, null, 2)),
                new Page(valueBlock(type, concatValues(LongStream.range(-100, 100), min, max))));
        JoinPagesIndex pagesIndex = new JoinPagesIndex.TestingFactory(false).newJoinPagesIndex(ImmutableList.of(type));
        buildPages.forEach(pagesIndex::addPage);
        LookupSource lookupSource = pagesIndex.createLookupSourceSupplier(TEST_SESSION, ImmutableList.of(0)).get();

        List<Long> buildValues = new ArrayList<>();
        for (Page page : buildPages) {
            buildValues.addAll(readValues(type, page.getBlock(0)));
        }

        Block probeValues = valueBlock(type, 5L, null, 7L, 42L, -100L, 3L, 0L, null, 120L, -1L);
        Block probeDictionary = DictionaryBlock.create(7, valueBlock(type, 42L, null, 5L, 9L, 7L), new int[] {4, 2, 0, 1, 3, 2, 4});
        List<Block> probeBlocks = ImmutableList.of(
                probeValues,
                probeValues.getRegion(2, 6),
                probeDictionary,
                probeDictionary.getRegion(1, 5),
                RunLengthEncodedBlock.create(type, 7L, 4),
                RunLengthEncodedBlock.create(type, 9L, 4),
                RunLengthEncodedBlock.create(type, null, 4),
                valueBlock(type, concatValues(LongStream.range(-110, 110), min, max, min + 1, max - 1)));
        for (Block probeBlock : probeBlocks) {
            assertProbe(type, lookupSource, buildValues, new Page(probeBlock));
        }
    }

    private static void assertProbe(Type type, LookupSource lookupSource, List<Long> buildValues, Page probePage)
    {
        Block probeBlock = probePage.getBlock(0);
        int[] positions = IntStream.range(0, probePage.getPositionCount())
                .filter(position -> !probeBlock.isNull(position))
                .toArray();
        long[] batchJoinPositions = new long[probePage.getPositionCount()];
        lookupSource.getJoinPosition(positions, probePage, probePage, new long[positions.length], batchJoinPositions);

        for (int position : positions) {
            Long probeValue = type.getLong(probeBlock, position);
            Set<Long> expected = LongStream.range(0, buildValues.size())
                    .filter(address -> Objects.equals(buildValues.get(toIntExact(address)), probeValue))
                    .boxed()
                    .collect(toImmutableSet());
            assertThat(joinPositions(lookupSource, batchJoinPositions[position], position, probePage)).isEqualTo(expected);
            assertThat(joinPositions(lookupSource, lookupSource.getJoinPosition(position, probePage, probePage), position, probePage)).isEqualTo(expected);
        }
    }

    private static Set<Long> joinPositions(LookupSource lookupSource, long firstJoinPosition, int probePosition, Page probePage)
    {
        ImmutableSet.Builder<Long> joinPositions = ImmutableSet.builder();
        long joinPosition = firstJoinPosition;
        while (joinPosition >= 0) {
            joinPositions.add(joinPosition);
            joinPosition = lookupSource.getNextJoinPosition(joinPosition, probePosition, probePage);
        }
        return joinPositions.build();
    }

    private static List<Long> readValues(Type type, Block block)
    {
        List<Long> values = new ArrayList<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                values.add(null);
            }
            else {
                values.add(type.getLong(block, position));
            }
        }
        return values;
    }

    private static Long[] concatValues(LongStream values, long... extraValues)
    {
        return LongStream.concat(values, LongStream.of(extraValues)).boxed().toArray(Long[]::new);
    }

    private static Block valueBlock(Type type, Long... values)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, values.length);
        for (Long value : Arrays.asList(values)) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                type.writeLong(blockBuilder, value);
            }
        }
        return blockBuilder.buildValueBlock();
    }
}
