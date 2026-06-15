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
package io.trino.operator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.DataSize;
import io.trino.FeaturesConfig;
import io.trino.Session;
import io.trino.geospatial.Rectangle;
import io.trino.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import io.trino.operator.join.BlockPositionIndex;
import io.trino.operator.join.JoinHashSupplier;
import io.trino.operator.join.LookupSource;
import io.trino.operator.join.LookupSourceSupplier;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import io.trino.sql.gen.JoinCompiler;
import io.trino.sql.gen.JoinCompiler.LookupSourceSupplierFactory;
import io.trino.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.trino.operator.HashArraySizeSupplier.defaultHashArraySizeSupplier;
import static io.trino.operator.join.JoinUtils.getSingleBigintJoinChannel;
import static io.trino.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static java.util.Objects.requireNonNull;

/**
 * Build-side index for hash and spatial joins. Specialized variant of {@link PagesIndex} that
 * holds only the channels needed to build a lookup source. It resolves build-row ordinals through
 * {@link io.trino.operator.join.BlockPositionIndex} (built from per-page position counts), so it
 * carries none of {@link PagesIndex}'s sort surface and no per-row address {@code long[]}.
 */
public class JoinPagesIndex
{
    private static final int INSTANCE_SIZE = instanceSize(JoinPagesIndex.class);

    private final JoinCompiler joinCompiler;
    private final NullSafeHashCompiler hashCompiler;

    private final List<Type> types;
    private final ObjectArrayList<Block>[] channels;
    private final IntArrayList positionCounts;
    private final boolean eagerCompact;

    // Directory resolving build-row ordinals to (block, position); built lazily from positionCounts once the build is done.
    private BlockPositionIndex blockPositionIndex;

    private int modificationCount;
    private int pageCount;
    private int nextBlockToCompact;
    private int positionCount;
    private long pagesMemorySize;
    private long estimatedSize;

    private JoinPagesIndex(
            JoinCompiler joinCompiler,
            NullSafeHashCompiler hashCompiler,
            List<Type> types,
            boolean eagerCompact)
    {
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.hashCompiler = requireNonNull(hashCompiler, "hashCompiler is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.eagerCompact = eagerCompact;

        //noinspection unchecked
        channels = (ObjectArrayList<Block>[]) new ObjectArrayList[types.size()];
        for (int i = 0; i < channels.length; i++) {
            channels[i] = ObjectArrayList.wrap(new Block[1024], 0);
        }

        positionCounts = new IntArrayList(1024);

        estimatedSize = calculateEstimatedSize();
    }

    public interface Factory
    {
        JoinPagesIndex newJoinPagesIndex(List<Type> types);
    }

    public static class TestingFactory
            implements Factory
    {
        private static final NullSafeHashCompiler NULL_SAFE_HASH_COMPILER = new NullSafeHashCompiler(PagesIndex.TestingFactory.TYPE_OPERATORS);
        private final JoinCompiler joinCompiler;
        private final boolean eagerCompact;

        public TestingFactory(boolean eagerCompact)
        {
            this(eagerCompact, true);
        }

        public TestingFactory(boolean eagerCompact, boolean enableSingleChannelBigintLookupSource)
        {
            this.eagerCompact = eagerCompact;
            joinCompiler = new JoinCompiler(PagesIndex.TestingFactory.TYPE_OPERATORS, enableSingleChannelBigintLookupSource);
        }

        @Override
        public JoinPagesIndex newJoinPagesIndex(List<Type> types)
        {
            return new JoinPagesIndex(joinCompiler, NULL_SAFE_HASH_COMPILER, types, eagerCompact);
        }
    }

    public static class DefaultFactory
            implements Factory
    {
        private final JoinCompiler joinCompiler;
        private final NullSafeHashCompiler hashCompiler;
        private final boolean eagerCompact;

        @Inject
        public DefaultFactory(JoinCompiler joinCompiler, NullSafeHashCompiler hashCompiler, FeaturesConfig featuresConfig)
        {
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.hashCompiler = requireNonNull(hashCompiler, "hashCompiler is null");
            this.eagerCompact = featuresConfig.isPagesIndexEagerCompactionEnabled();
        }

        @Override
        public JoinPagesIndex newJoinPagesIndex(List<Type> types)
        {
            return new JoinPagesIndex(joinCompiler, hashCompiler, types, eagerCompact);
        }
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public ObjectArrayList<Block> getChannel(int channel)
    {
        return channels[channel];
    }

    public Type getType(int channel)
    {
        return types.get(channel);
    }

    public void clear()
    {
        modificationCount++;
        for (ObjectArrayList<Block> channel : channels) {
            channel.clear();
            channel.trim();
        }
        positionCount = 0;
        nextBlockToCompact = 0;
        pagesMemorySize = 0;
        positionCounts.clear();
        positionCounts.trim();
        blockPositionIndex = null;
        pageCount = 0;

        estimatedSize = calculateEstimatedSize();
    }

    public void addPage(Page page)
    {
        modificationCount++;
        // ignore empty pages
        if (page.getPositionCount() == 0) {
            return;
        }

        // the lookup source uses a long[] internally, so cap size to a nice round number for safety
        int resultingSize = positionCount + page.getPositionCount();
        if (resultingSize < 0 || resultingSize >= 2_000_000_000) {
            throw new TrinoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of pages index cannot exceed 2 billion entries");
        }

        pageCount++;
        positionCount = resultingSize;
        positionCounts.add(page.getPositionCount());
        blockPositionIndex = null;

        for (int i = 0; i < channels.length; i++) {
            Block block = page.getBlock(i);
            if (eagerCompact) {
                block = block.copyRegion(0, block.getPositionCount());
            }
            channels[i].add(block);
            pagesMemorySize += block.getRetainedSizeInBytes();
        }

        estimatedSize = calculateEstimatedSize();
    }

    public DataSize getEstimatedSize()
    {
        return DataSize.ofBytes(estimatedSize);
    }

    public void compact()
    {
        modificationCount++;
        if (eagerCompact || channels.length == 0) {
            return;
        }
        for (int channel = 0; channel < types.size(); channel++) {
            ObjectArrayList<Block> blocks = channels[channel];
            for (int i = nextBlockToCompact; i < blocks.size(); i++) {
                Block block = blocks.get(i);

                // Copy the block to compact its size
                Block compactedBlock = block.copyRegion(0, block.getPositionCount());
                blocks.set(i, compactedBlock);
                pagesMemorySize -= block.getRetainedSizeInBytes();
                pagesMemorySize += compactedBlock.getRetainedSizeInBytes();
            }
        }
        nextBlockToCompact = channels[0].size();
        estimatedSize = calculateEstimatedSize();
    }

    private BlockPositionIndex getBlockPositionIndex()
    {
        if (blockPositionIndex == null) {
            blockPositionIndex = new BlockPositionIndex(positionCounts);
        }
        return blockPositionIndex;
    }

    private long calculateEstimatedSize()
    {
        long elementsSize = (channels.length > 0) ? sizeOf(channels[0].elements()) : 0;
        long channelsArraySize = elementsSize * channels.length;
        long positionCountsSize = sizeOf(positionCounts.elements());
        return INSTANCE_SIZE + pagesMemorySize + channelsArraySize + positionCountsSize;
    }

    public Supplier<LookupSource> createLookupSourceSupplier(Session session, List<Integer> joinChannels)
    {
        return createLookupSourceSupplier(session, joinChannels, Optional.empty(), OptionalInt.empty(), ImmutableList.of());
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            OptionalInt sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories)
    {
        return createLookupSourceSupplier(session, joinChannels, filterFunctionFactory, sortChannel, searchFunctionFactories, Optional.empty(), defaultHashArraySizeSupplier());
    }

    public PagesSpatialIndexSupplier createPagesSpatialIndex(
            Session session,
            int geometryChannel,
            OptionalInt radiusChannel,
            OptionalDouble constantRadius,
            OptionalInt partitionChannel,
            SpatialPredicate spatialRelationshipTest,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            List<Integer> outputChannels,
            Map<Integer, Rectangle> partitions)
    {
        // TODO probably shouldn't copy to reduce memory and for memory accounting's sake
        List<ObjectArrayList<Block>> channels = ImmutableList.copyOf(this.channels);
        return new PagesSpatialIndexSupplier(session, getBlockPositionIndex(), outputChannels, channels, geometryChannel, radiusChannel, constantRadius, partitionChannel, spatialRelationshipTest, filterFunctionFactory, partitions);
    }

    public LookupSourceSupplier createLookupSourceSupplier(
            Session session,
            List<Integer> joinChannels,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            OptionalInt sortChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            Optional<List<Integer>> outputChannels,
            HashArraySizeSupplier hashArraySizeSupplier)
    {
        List<ObjectArrayList<Block>> channels = ImmutableList.copyOf(this.channels);
        LookupSourceSupplierFactory lookupSourceFactory = joinCompiler.compileLookupSourceFactory(types, joinChannels, sortChannel, outputChannels);
        int[] joinChannelsArray = joinChannels.stream().mapToInt(Integer::intValue).toArray();
        List<Type> joinChannelTypes = joinChannels.stream().map(types::get).collect(toImmutableList());
        InterpretedHashGenerator hashGenerator = InterpretedHashGenerator.createChannelsHashGenerator(
                joinChannelTypes, joinChannelsArray, hashCompiler);
        return lookupSourceFactory.createLookupSourceSupplier(
                session,
                channels,
                getBlockPositionIndex(),
                filterFunctionFactory,
                sortChannel,
                searchFunctionFactories,
                hashArraySizeSupplier,
                joinChannels,
                hashGenerator);
    }

    public long getEstimatedMemoryRequiredToCreateLookupSource(
            HashArraySizeSupplier hashArraySizeSupplier,
            OptionalInt sortChannel,
            List<Integer> joinChannels)
    {
        // channels are shared between JoinPagesIndex and JoinHashSupplier and are accounted as part of lookupSourceEstimatedRetainedSizeInBytes
        long lookupSourceEstimatedRetainedSizeInBytes = JoinHashSupplier.getEstimatedRetainedSizeInBytes(
                positionCount,
                ImmutableList.copyOf(channels),
                pagesMemorySize,
                sortChannel,
                getSingleBigintJoinChannel(joinChannels, types),
                hashArraySizeSupplier);
        // JoinPagesIndex is retained during LookupSource creation, hence any extra memory retained by it must be accounted here
        long pagesIndexAdditionalRetainedSizeInBytes = getExtraPagesIndexMemoryWithLookupSourceBuild();
        return pagesIndexAdditionalRetainedSizeInBytes + lookupSourceEstimatedRetainedSizeInBytes;
    }

    public long getExtraPagesIndexMemoryWithLookupSourceBuild()
    {
        return INSTANCE_SIZE + sizeOf(positionCounts.elements());
    }

    public Iterator<Page> getPages()
    {
        return new AbstractIterator<>()
        {
            private final int startingModificationCount = modificationCount;
            private int currentPage;

            @Override
            protected Page computeNext()
            {
                if (currentPage == pageCount) {
                    if (startingModificationCount != modificationCount) {
                        throw new ConcurrentModificationException("JoinPagesIndex mutated during iteration: %s != %s".formatted(startingModificationCount, modificationCount));
                    }
                    return endOfData();
                }

                int positions = positionCounts.getInt(currentPage);
                Block[] blocks = Stream.of(channels)
                        .map(channel -> channel.get(currentPage))
                        .toArray(Block[]::new);

                currentPage++;
                return new Page(positions, blocks);
            }
        };
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", positionCount)
                .add("types", types)
                .add("estimatedSize", estimatedSize)
                .toString();
    }
}
