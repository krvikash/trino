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
import io.trino.operator.join.LookupSource;
import io.trino.spi.Page;
import io.trino.spi.PageCapacityEstimator;
import io.trino.spi.block.Block;
import io.trino.spi.block.PreSizedBlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static io.trino.operator.join.LookupSource.JOIN_POSITION_NOT_FOUND;
import static io.trino.operator.project.PageProcessor.MAX_BATCH_SIZE;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

/**
 * Builds output pages for non-spilling lookup joins.
 * <p>
 * Probe-side columns reuse the source block directly when the probe indices
 * cover the whole block, a {@code getRegion} slice when they are contiguous
 * but partial, and a dictionary block otherwise.
 * <p>
 * Build-side columns are recorded as join positions during {@link #appendRow}
 * and {@link #appendNullForBuild} and materialized at {@link #build} via
 * {@link PreSizedBlockBuilder}; the {@link LookupSource#JOIN_POSITION_NOT_FOUND}
 * sentinel produces nulls for unmatched outer-join rows.
 * <p>
 * TODO use dictionary blocks (probably an extended kind) to avoid copying the
 *      build side entirely.
 */
public class LookupJoinPageBuilder
{
    private final IntArrayList probeIndexBuilder = new IntArrayList();
    private final LongArrayList buildPositions = new LongArrayList();
    private final List<Type> buildTypes;
    private final int buildOutputChannelCount;
    private final PageCapacityEstimator capacityEstimator;
    private int previousPosition = -1;
    private boolean isSequentialProbeIndices = true;
    private boolean repeatBuildRow;

    public LookupJoinPageBuilder(List<Type> buildTypes)
    {
        this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));
        this.buildOutputChannelCount = this.buildTypes.size();
        // Start the position estimate low; recordPage grows it toward MAX_BATCH_SIZE
        // for narrow rows and shrinks it for wide rows to honor the page byte budget.
        this.capacityEstimator = new PageCapacityEstimator(MAX_BATCH_SIZE / 16, MAX_BATCH_SIZE, DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
    }

    public boolean isFull()
    {
        return capacityEstimator.isFull(probeIndexBuilder.size());
    }

    public boolean isEmpty()
    {
        return probeIndexBuilder.isEmpty();
    }

    public int getPositionCount()
    {
        // when build rows are repeated then position count is equal to probe position count
        verify(!repeatBuildRow);
        return probeIndexBuilder.size();
    }

    public void reset()
    {
        // be aware that probeIndexBuilder and buildPositions will not clear their capacity
        probeIndexBuilder.clear();
        buildPositions.clear();
        previousPosition = -1;
        isSequentialProbeIndices = true;
        repeatBuildRow = false;
    }

    /**
     * append the index for the probe and record the build position for deferred materialization
     */
    public void appendRow(JoinProbe probe, long joinPosition)
    {
        appendProbeIndex(probe);
        buildPositions.add(joinPosition);
    }

    /**
     * append the index for the probe and a sentinel for the build (null padding at flush time)
     */
    public void appendNullForBuild(JoinProbe probe)
    {
        appendProbeIndex(probe);
        buildPositions.add(JOIN_POSITION_NOT_FOUND);
    }

    public void repeatBuildRow()
    {
        repeatBuildRow = true;
    }

    public Page build(JoinProbe probe, LookupSource lookupSource)
    {
        if (repeatBuildRow) {
            return buildRepeatedPage(probe, lookupSource);
        }

        int outputPositions = probeIndexBuilder.size();
        verify(buildPositions.size() == outputPositions);

        int[] probeOutputChannels = probe.getOutputChannels();
        Block[] blocks = new Block[probeOutputChannels.length + buildOutputChannelCount];
        Page probePage = probe.getPage();
        if (!isSequentialProbeIndices || outputPositions == 0) {
            int[] probeIndices = probeIndexBuilder.toIntArray();
            for (int i = 0; i < probeOutputChannels.length; i++) {
                blocks[i] = probePage.getBlock(probeOutputChannels[i]).getPositions(probeIndices, 0, outputPositions);
            }
        }
        else {
            // probeIndices are sequential without holes
            int startRegion = probeIndexBuilder.getInt(0);
            verify(previousPosition - startRegion == outputPositions - 1);
            // probeIndices are a simple covering of the block, output the probe block directly
            boolean outputProbeBlocksDirectly = startRegion == 0 && outputPositions == probePage.getPositionCount();

            for (int i = 0; i < probeOutputChannels.length; i++) {
                Block block = probePage.getBlock(probeOutputChannels[i]);
                if (!outputProbeBlocksDirectly) {
                    // only a subregion of the block should be output
                    block = block.getRegion(startRegion, outputPositions);
                }
                blocks[i] = block;
            }
        }

        PreSizedBlockBuilder[] builders = createBuilders(outputPositions);
        lookupSource.appendTo(buildPositions.elements(), 0, outputPositions, builders);
        int offset = probeOutputChannels.length;
        for (int i = 0; i < buildOutputChannelCount; i++) {
            blocks[offset + i] = builders[i].build();
            verify(blocks[offset + i].getPositionCount() == outputPositions);
        }
        Page page = new Page(outputPositions, blocks);
        capacityEstimator.recordPage(page.getSizeInBytes(), outputPositions);
        return page;
    }

    private Page buildRepeatedPage(JoinProbe probe, LookupSource lookupSource)
    {
        // Build match can be repeated only if there is a single build row match
        // and probe join channels are run length encoded.
        verify(probe.areProbeJoinChannelsRunLengthEncoded());
        verify(probeIndexBuilder.size() == 1);
        verify(buildPositions.size() == 1);
        verify(probeIndexBuilder.getInt(0) == 0);

        int positionCount = probe.getPage().getPositionCount();
        int[] probeOutputChannels = probe.getOutputChannels();
        Block[] blocks = new Block[probeOutputChannels.length + buildOutputChannelCount];

        for (int i = 0; i < probeOutputChannels.length; i++) {
            blocks[i] = probe.getPage().getBlock(probeOutputChannels[i]);
        }

        PreSizedBlockBuilder[] builders = createBuilders(1);
        lookupSource.appendTo(buildPositions.elements(), 0, 1, builders);
        int offset = probeOutputChannels.length;
        for (int i = 0; i < buildOutputChannelCount; i++) {
            blocks[offset + i] = RunLengthEncodedBlock.create(builders[i].build(), positionCount);
        }

        return new Page(positionCount, blocks);
    }

    private PreSizedBlockBuilder[] createBuilders(int expectedEntries)
    {
        PreSizedBlockBuilder[] builders = new PreSizedBlockBuilder[buildOutputChannelCount];
        for (int i = 0; i < buildOutputChannelCount; i++) {
            builders[i] = buildTypes.get(i).createPreSizedBlockBuilder(expectedEntries);
        }
        return builders;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", probeIndexBuilder.size())
                .toString();
    }

    private void appendProbeIndex(JoinProbe probe)
    {
        int position = probe.getPosition();
        // positions to be appended should be in ascending order
        verify(position >= 0 && previousPosition <= position);
        isSequentialProbeIndices &= position == previousPosition + 1 || previousPosition == -1;
        previousPosition = position;
        probeIndexBuilder.add(position);
    }
}
