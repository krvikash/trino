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
package io.trino.operator.join.nonspilling;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.operator.NullSafeHashCompiler;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.nonspilling.JoinProbe.JoinProbeFactory;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.block.PreSizedBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.operator.InterpretedHashGenerator.createPagePrefixHashGenerator;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLookupJoinPageBuilder
{
    private static final NullSafeHashCompiler HASH_COMPILER = new NullSafeHashCompiler(new TypeOperators());

    @Test
    public void testPageBuilder()
    {
        int entries = 10_000;
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block, block);

        List<Type> types = ImmutableList.of(BIGINT, BIGINT);
        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(ImmutableList.of(0, 1), ImmutableList.of(0, 1), false, createPagePrefixHashGenerator(types, HASH_COMPILER));
        LookupSource lookupSource = new TestLookupSource(types, page);
        JoinProbe probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(types);

        int joinPosition = 0;
        while (!lookupJoinPageBuilder.isFull() && probe.advanceNextPosition()) {
            lookupJoinPageBuilder.appendRow(probe, joinPosition++);
            lookupJoinPageBuilder.appendNullForBuild(probe);
        }
        assertThat(lookupJoinPageBuilder.isEmpty()).isFalse();

        Page output = lookupJoinPageBuilder.build(probe, lookupSource);
        assertThat(output.getChannelCount()).isEqualTo(4);
        assertThat(output.getBlock(0)).isInstanceOf(DictionaryBlock.class);
        assertThat(output.getBlock(1)).isInstanceOf(DictionaryBlock.class);
        for (int i = 0; i < output.getPositionCount(); i++) {
            assertThat(output.getBlock(0).isNull(i)).isFalse();
            assertThat(output.getBlock(1).isNull(i)).isFalse();
            assertThat(BIGINT.getLong(output.getBlock(0), i)).isEqualTo(i / 2);
            assertThat(BIGINT.getLong(output.getBlock(1), i)).isEqualTo(i / 2);
            if (i % 2 == 0) {
                assertThat(output.getBlock(2).isNull(i)).isFalse();
                assertThat(output.getBlock(3).isNull(i)).isFalse();
                assertThat(BIGINT.getLong(output.getBlock(2), i)).isEqualTo(i / 2);
                assertThat(BIGINT.getLong(output.getBlock(3), i)).isEqualTo(i / 2);
            }
            else {
                assertThat(output.getBlock(2).isNull(i)).isTrue();
                assertThat(output.getBlock(3).isNull(i)).isTrue();
            }
        }
        assertThat(lookupJoinPageBuilder.toString()).contains("positionCount=" + output.getPositionCount());

        lookupJoinPageBuilder.reset();
        assertThat(lookupJoinPageBuilder.isEmpty()).isTrue();
    }

    @Test
    public void testDifferentPositions()
    {
        int entries = 100;
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block);
        List<Type> types = ImmutableList.of(BIGINT);
        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(ImmutableList.of(0), ImmutableList.of(0), false, createPagePrefixHashGenerator(types, HASH_COMPILER));
        LookupSource lookupSource = new TestLookupSource(types, page);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(types);

        // empty
        JoinProbe probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        Page output = lookupJoinPageBuilder.build(probe, lookupSource);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getBlock(0)).isInstanceOf(LongArrayBlock.class);
        assertThat(output.getPositionCount()).isEqualTo(0);
        // build block must be a real empty block, not a null slot
        assertThat(output.getBlock(1)).isNotNull();
        assertThat(output.getBlock(1).getPositionCount()).isEqualTo(0);
        lookupJoinPageBuilder.reset();

        // the probe covers non-sequential positions
        probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        for (int joinPosition = 0; probe.advanceNextPosition(); joinPosition++) {
            if (joinPosition % 2 == 1) {
                continue;
            }
            lookupJoinPageBuilder.appendRow(probe, joinPosition);
        }
        output = lookupJoinPageBuilder.build(probe, lookupSource);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getBlock(0)).isInstanceOf(DictionaryBlock.class);
        assertThat(output.getPositionCount()).isEqualTo(entries / 2);
        for (int i = 0; i < entries / 2; i++) {
            assertThat(BIGINT.getLong(output.getBlock(0), i)).isEqualTo(i * 2L);
            assertThat(BIGINT.getLong(output.getBlock(1), i)).isEqualTo(i * 2L);
        }
        lookupJoinPageBuilder.reset();

        // the probe covers everything
        probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        for (int joinPosition = 0; probe.advanceNextPosition(); joinPosition++) {
            lookupJoinPageBuilder.appendRow(probe, joinPosition);
        }
        output = lookupJoinPageBuilder.build(probe, lookupSource);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getBlock(0)).isNotInstanceOf(DictionaryBlock.class);
        assertThat(output.getPositionCount()).isEqualTo(entries);
        for (int i = 0; i < entries; i++) {
            assertThat(BIGINT.getLong(output.getBlock(0), i)).isEqualTo(i);
            assertThat(BIGINT.getLong(output.getBlock(1), i)).isEqualTo(i);
        }
        lookupJoinPageBuilder.reset();

        // the probe covers some sequential positions
        probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        for (int joinPosition = 0; probe.advanceNextPosition(); joinPosition++) {
            if (joinPosition < 10 || joinPosition >= 50) {
                continue;
            }
            lookupJoinPageBuilder.appendRow(probe, joinPosition);
        }
        output = lookupJoinPageBuilder.build(probe, lookupSource);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getBlock(0)).isNotInstanceOf(DictionaryBlock.class);
        assertThat(output.getPositionCount()).isEqualTo(40);
        for (int i = 10; i < 50; i++) {
            assertThat(BIGINT.getLong(output.getBlock(0), i - 10)).isEqualTo(i);
            assertThat(BIGINT.getLong(output.getBlock(1), i - 10)).isEqualTo(i);
        }
    }

    @Test
    public void testCrossJoinWithEmptyBuild()
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(blockBuilder, 0);
        Page page = new Page(blockBuilder.build());

        // nothing on the build side so we don't append anything
        LookupSource lookupSource = new TestLookupSource(ImmutableList.of(), page);
        List<Type> types = ImmutableList.of(BIGINT);
        JoinProbe probe = new JoinProbeFactory(ImmutableList.of(0), ImmutableList.of(0), false, createPagePrefixHashGenerator(types, HASH_COMPILER)).createJoinProbe(page, lookupSource);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(types);

        // append the same row many times should also flush in the end
        probe.advanceNextPosition();
        for (int i = 0; i < 300_000 && !lookupJoinPageBuilder.isFull(); i++) {
            lookupJoinPageBuilder.appendRow(probe, 0);
        }
        assertThat(lookupJoinPageBuilder.isFull()).isTrue();
    }

    @Test
    public void testAllNullForBuild()
    {
        int entries = 100;
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(entries);
        for (int i = 0; i < entries; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block);

        List<Type> types = ImmutableList.of(BIGINT);
        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(ImmutableList.of(0), ImmutableList.of(0), false, createPagePrefixHashGenerator(types, HASH_COMPILER));
        LookupSource lookupSource = new TestLookupSource(types, page);
        JoinProbe probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(types);

        while (probe.advanceNextPosition()) {
            lookupJoinPageBuilder.appendNullForBuild(probe);
        }
        Page output = lookupJoinPageBuilder.build(probe, lookupSource);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getPositionCount()).isEqualTo(entries);
        for (int i = 0; i < entries; i++) {
            assertThat(output.getBlock(0).isNull(i)).isFalse();
            assertThat(BIGINT.getLong(output.getBlock(0), i)).isEqualTo(i);
            assertThat(output.getBlock(1).isNull(i)).isTrue();
        }
    }

    @Test
    public void testRepeatBuildRow()
    {
        int positionCount = 50;
        Block probeBlock = RunLengthEncodedBlock.create(BIGINT, 7L, positionCount);
        Page probePage = new Page(probeBlock);

        BlockBuilder buildBlockBuilder = BIGINT.createFixedSizeBlockBuilder(1);
        BIGINT.writeLong(buildBlockBuilder, 42);
        Page buildPage = new Page(buildBlockBuilder.build());

        List<Type> types = ImmutableList.of(BIGINT);
        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(ImmutableList.of(0), ImmutableList.of(0), false, createPagePrefixHashGenerator(types, HASH_COMPILER));
        LookupSource lookupSource = new TestLookupSource(types, buildPage);
        JoinProbe probe = joinProbeFactory.createJoinProbe(probePage, lookupSource);
        assertThat(probe.areProbeJoinChannelsRunLengthEncoded()).isTrue();
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(types);

        probe.advanceNextPosition();
        lookupJoinPageBuilder.appendRow(probe, 0);
        lookupJoinPageBuilder.repeatBuildRow();

        Page output = lookupJoinPageBuilder.build(probe, lookupSource);
        assertThat(output.getChannelCount()).isEqualTo(2);
        assertThat(output.getPositionCount()).isEqualTo(positionCount);
        assertThat(output.getBlock(0)).isInstanceOf(RunLengthEncodedBlock.class);
        assertThat(output.getBlock(1)).isInstanceOf(RunLengthEncodedBlock.class);
        for (int i = 0; i < positionCount; i++) {
            assertThat(BIGINT.getLong(output.getBlock(0), i)).isEqualTo(7L);
            assertThat(BIGINT.getLong(output.getBlock(1), i)).isEqualTo(42L);
        }
    }

    @Test
    public void testCapacityShrinksForWideBuild()
    {
        // Wide VARCHAR build column: each row contributes ~2 KB of payload, so a batch
        // sized at the initial capacity (MAX_BATCH_SIZE / 16 = 512 rows) is ~1 MiB,
        // above the 1 MiB output budget. After the first flush the estimator
        // should shrink subsequent batches below the initial capacity.
        int sourcePositions = MAX_BATCH_SIZE;
        Slice wide = Slices.utf8Slice("x".repeat(2048));
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, sourcePositions);
        for (int i = 0; i < sourcePositions; i++) {
            VARCHAR.writeSlice(blockBuilder, wide);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block);

        List<Type> types = ImmutableList.of(VARCHAR);
        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(ImmutableList.of(0), ImmutableList.of(0), false, createPagePrefixHashGenerator(types, HASH_COMPILER));
        LookupSource lookupSource = new TestLookupSource(types, page);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(types);

        JoinProbe probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        long joinPosition = 0;
        while (!lookupJoinPageBuilder.isFull() && probe.advanceNextPosition()) {
            lookupJoinPageBuilder.appendRow(probe, joinPosition++);
        }
        int firstBatchSize = lookupJoinPageBuilder.getPositionCount();
        assertThat(firstBatchSize).isEqualTo(MAX_BATCH_SIZE / 16);
        Page firstPage = lookupJoinPageBuilder.build(probe, lookupSource);
        assertThat(firstPage.getSizeInBytes()).isGreaterThan(1_000_000L);
        lookupJoinPageBuilder.reset();

        probe = joinProbeFactory.createJoinProbe(page, lookupSource);
        while (!lookupJoinPageBuilder.isFull() && probe.advanceNextPosition()) {
            lookupJoinPageBuilder.appendRow(probe, joinPosition++);
        }
        int secondBatchSize = lookupJoinPageBuilder.getPositionCount();
        assertThat(secondBatchSize).isLessThan(firstBatchSize);
    }

    @Test
    public void testCapacityGrowsForNarrowBuild()
    {
        // BIGINT build column: ~8 bytes per row leaves the byte budget non-binding,
        // so successive batches should double in capacity until they reach MAX_BATCH_SIZE.
        int sourcePositions = MAX_BATCH_SIZE * 4;
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(sourcePositions);
        for (int i = 0; i < sourcePositions; i++) {
            BIGINT.writeLong(blockBuilder, i);
        }
        Block block = blockBuilder.build();
        Page page = new Page(block);

        List<Type> types = ImmutableList.of(BIGINT);
        JoinProbeFactory joinProbeFactory = new JoinProbeFactory(ImmutableList.of(0), ImmutableList.of(0), false, createPagePrefixHashGenerator(types, HASH_COMPILER));
        LookupSource lookupSource = new TestLookupSource(types, page);
        LookupJoinPageBuilder lookupJoinPageBuilder = new LookupJoinPageBuilder(types);

        long joinPosition = 0;
        int previousBatchSize = 0;
        for (int batch = 0; batch < 6; batch++) {
            JoinProbe probe = joinProbeFactory.createJoinProbe(page, lookupSource);
            while (!lookupJoinPageBuilder.isFull() && probe.advanceNextPosition()) {
                lookupJoinPageBuilder.appendRow(probe, joinPosition++);
            }
            int batchSize = lookupJoinPageBuilder.getPositionCount();
            if (batch == 0) {
                assertThat(batchSize).isEqualTo(MAX_BATCH_SIZE / 16);
            }
            else {
                assertThat(batchSize).isGreaterThanOrEqualTo(previousBatchSize);
            }
            previousBatchSize = batchSize;
            lookupJoinPageBuilder.build(probe, lookupSource);
            lookupJoinPageBuilder.reset();
        }
        assertThat(previousBatchSize).isEqualTo(MAX_BATCH_SIZE);
    }

    private static final class TestLookupSource
            implements LookupSource
    {
        private final List<Type> types;
        private final Page page;

        public TestLookupSource(List<Type> types, Page page)
        {
            this.types = types;
            this.page = page;
        }

        @Override
        public boolean isEmpty()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPositionCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long joinPositionWithinPartition(long joinPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page page, Page allChannelsPage, long rawHash)
        {
            return -1;
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
        {
            return 0;
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            for (int i = 0; i < types.size(); i++) {
                Block block = page.getBlock(i);
                pageBuilder.getBlockBuilder(i).append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition((int) position));
            }
        }

        @Override
        public void appendTo(long position, PreSizedBlockBuilder[] builders)
        {
            for (int i = 0; i < types.size(); i++) {
                Block block = page.getBlock(i);
                builders[i].append(block.getUnderlyingValueBlock(), block.getUnderlyingValuePosition((int) position));
            }
        }

        @Override
        public void close() {}
    }
}
