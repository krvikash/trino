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
package io.trino.operator.torin.window;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.GroupByOptions;
import ai.rapids.cudf.GroupByScanAggregation;
import ai.rapids.cudf.GroupByScanAggregationOnColumn;
import ai.rapids.cudf.OutOfBoundsPolicy;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.ScanAggregation;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import io.trino.operator.torin.GpuOperation;
import io.trino.operator.torin.memory.AllocatedMemory;
import io.trino.plugin.base.torin.ClosingRef;
import io.trino.plugin.base.torin.TablesList;
import io.trino.plugin.base.torin.UncheckedCloser;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.torin.Column;
import io.trino.spi.torin.Column.DeviceMemory;
import io.trino.spi.torin.GpuPage;
import io.trino.spi.torin.MemoryAmount;
import io.trino.spi.torin.borrow.Borrow;
import io.trino.spi.torin.borrow.Move;
import io.trino.spi.torin.borrow.Own;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.FrameBoundType;
import io.trino.sql.planner.plan.WindowFrameType;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import static ai.rapids.cudf.BinaryOp.ADD;
import static ai.rapids.cudf.BinaryOp.NULL_MAX;
import static ai.rapids.cudf.BinaryOp.NULL_MIN;
import static ai.rapids.cudf.BinaryOp.SUB;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.torin.GpuSortUtils.createNanSortKey;
import static io.trino.operator.torin.GpuSortUtils.estimateOrderByMemoryUsage;
import static io.trino.operator.torin.GpuSortUtils.isNullSmallest;
import static io.trino.operator.torin.GpuSortUtils.needsNanSortKey;
import static io.trino.operator.torin.GpuSortUtils.orderBy;
import static io.trino.operator.torin.memory.GpuMemoryUtils.nullMaskBytes;
import static io.trino.plugin.base.torin.GpuUtils.closeColumns;
import static io.trino.plugin.base.torin.GpuUtils.selectColumns;
import static io.trino.plugin.base.torin.GpuUtils.toGpuPage;
import static io.trino.plugin.base.torin.GpuUtils.toTable;
import static io.trino.sql.planner.plan.FrameBoundType.CURRENT_ROW;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_FOLLOWING;
import static io.trino.sql.planner.plan.FrameBoundType.UNBOUNDED_PRECEDING;
import static io.trino.sql.planner.plan.WindowFrameType.RANGE;
import static io.trino.sql.planner.plan.WindowFrameType.ROWS;
import static java.lang.Math.addExact;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class GpuWindow
        implements GpuOperation
{
    private static final int INT32_BYTES = DType.INT32.getSizeInBytes();
    private static final int INT64_BYTES = DType.INT64.getSizeInBytes();
    private static final int BOOL8_BYTES = DType.BOOL8.getSizeInBytes();
    // cuDF's sort_groupby_helper (sort_helper.cu) allocates three shared INT32 columns: _key_sorted_order, _group_offsets, _group_labels.
    private static final int GROUPED_SCAN_SORT_HELPER_BYTES_PER_ROW = 3 * INT32_BYTES;

    public static class Factory
            implements GpuOperation.Factory
    {
        private final List<Type> partitionTypes;
        private final int[] partitionChannels;
        private final List<Type> orderTypes;
        private final int[] orderChannels;
        private final List<SortOrder> orderSortOrders;
        private final List<WindowComputation> computations;

        public Factory(
                List<Type> partitionTypes,
                int[] partitionChannels,
                List<Type> orderTypes,
                int[] orderChannels,
                List<SortOrder> orderSortOrders,
                List<WindowComputation> computations)
        {
            checkArgument(orderChannels.length == orderSortOrders.size(), "orderChannels and orderSortOrders sizes do not match");
            checkArgument(orderChannels.length == orderTypes.size(), "orderChannels and orderTypes sizes do not match");
            checkArgument(partitionChannels.length == partitionTypes.size(), "partitionChannels and partitionTypes sizes do not match");
            this.partitionTypes = ImmutableList.copyOf(partitionTypes);
            this.partitionChannels = partitionChannels.clone();
            this.orderTypes = ImmutableList.copyOf(orderTypes);
            this.orderChannels = orderChannels.clone();
            this.orderSortOrders = ImmutableList.copyOf(orderSortOrders);
            this.computations = ImmutableList.copyOf(requireNonNull(computations, "computations is null"));
        }

        @Override
        public Factory duplicate()
        {
            return new Factory(partitionTypes, partitionChannels, orderTypes, orderChannels, orderSortOrders, computations);
        }

        @Override
        public GpuOperation create(Context context, GpuOperation source)
        {
            return new GpuWindow(context, source, partitionTypes, partitionChannels, orderTypes, orderChannels, orderSortOrders, computations);
        }

        @Override
        public void noMoreOperators() {}
    }

    private final GpuOperation source;
    private final List<Type> partitionTypes;
    private final int[] partitionChannels;
    private final List<Type> orderTypes;
    private final int[] orderChannels;
    private final List<SortOrder> orderSortOrders;
    private final List<ScanComputation> scanComputations;
    private final int[] scanOutputIndexes;
    private final int[] prefixSumDifferenceOutputIndexes;
    // The distinct input channels of the frame sums, and the index into them of the input channel of each frame sum
    private final int[] prefixSumChannels;
    private final int[] prefixSumIndexes;
    private final List<FrameComputations> frameComputations;
    private final boolean hasRowsFrame;
    private final boolean hasRangeFrame;

    private final @Own TablesList bufferedTables = TablesList.create();
    private final ClosingRef<AllocatedMemory> allocated;
    private long totalBufferedRowCount;
    private boolean finished;

    public sealed interface WindowComputation {}

    public record ScanComputation(
            int inputChannel,
            ScanAggregation scanAggregation,
            GroupByScanAggregation groupedScanAggregation,
            int scanOutputBytesPerRow,
            int groupedScanWorkingBytesPerRow)
            implements WindowComputation
    {
        public ScanComputation
        {
            checkArgument(inputChannel >= 0, "inputChannel is negative");
            requireNonNull(scanAggregation, "scanAggregation is null");
            requireNonNull(groupedScanAggregation, "groupedScanAggregation is null");
            checkArgument(scanOutputBytesPerRow > 0, "scanOutputBytesPerRow must be positive");
            checkArgument(groupedScanWorkingBytesPerRow >= 0, "groupedScanWorkingBytesPerRow is negative");
        }
    }

    public record FrameBound(FrameBoundType type, OptionalInt offsetChannel)
    {
        public FrameBound
        {
            requireNonNull(type, "type is null");
            requireNonNull(offsetChannel, "offsetChannel is null");
            checkArgument(offsetChannel.isPresent() == isOffset(type), "offsetChannel must be present exactly for an offset bound");
            checkArgument(offsetChannel.orElse(0) >= 0, "offsetChannel is negative");
        }

        public static FrameBound unbounded(FrameBoundType type)
        {
            return new FrameBound(type, OptionalInt.empty());
        }

        public static FrameBound currentRow()
        {
            return new FrameBound(CURRENT_ROW, OptionalInt.empty());
        }

        public static FrameBound offset(FrameBoundType type, int offsetChannel)
        {
            return new FrameBound(type, OptionalInt.of(offsetChannel));
        }

        public boolean isOffset()
        {
            return isOffset(type);
        }

        private static boolean isOffset(FrameBoundType type)
        {
            return switch (type) {
                case PRECEDING, FOLLOWING -> true;
                case UNBOUNDED_PRECEDING, CURRENT_ROW, UNBOUNDED_FOLLOWING -> false;
            };
        }
    }

    public record WindowFrame(WindowFrameType type, FrameBound start, FrameBound end)
    {
        public static final WindowFrame DEFAULT = new WindowFrame(RANGE, FrameBound.unbounded(UNBOUNDED_PRECEDING), FrameBound.currentRow());

        public WindowFrame
        {
            requireNonNull(type, "type is null");
            requireNonNull(start, "start is null");
            requireNonNull(end, "end is null");
            checkArgument(start.type() != UNBOUNDED_FOLLOWING, "frame cannot start at UNBOUNDED FOLLOWING");
            checkArgument(end.type() != UNBOUNDED_PRECEDING, "frame cannot end at UNBOUNDED PRECEDING");
            // TODO: Support RANGE and GROUPS frames: https://starburstdata.atlassian.net/browse/ENG-28121
            checkArgument(
                    type == ROWS || (type == RANGE && start.type() == UNBOUNDED_PRECEDING && end.type() == CURRENT_ROW),
                    "Unsupported frame: %s BETWEEN %s AND %s",
                    type,
                    start.type(),
                    end.type());
        }

        public boolean hasOffset()
        {
            return start.isOffset() || end.isOffset();
        }
    }

    public record PrefixSumDifferenceComputation(int inputChannel, WindowFrame frame)
            implements WindowComputation
    {
        public PrefixSumDifferenceComputation
        {
            checkArgument(inputChannel >= 0, "inputChannel is negative");
            requireNonNull(frame, "frame is null");
        }
    }

    // The prefix sum differences over one frame: the indexes of their computations, and the distinct prefix sums they
    // take the differences of
    private record FrameComputations(WindowFrame frame, int[] computationIndexes, int[] prefixSumIndexes)
    {
        public FrameComputations
        {
            requireNonNull(frame, "frame is null");
            computationIndexes = computationIndexes.clone();
            prefixSumIndexes = prefixSumIndexes.clone();
        }
    }

    private GpuWindow(
            Context context,
            GpuOperation source,
            List<Type> partitionTypes,
            int[] partitionChannels,
            List<Type> orderTypes,
            int[] orderChannels,
            List<SortOrder> orderSortOrders,
            List<WindowComputation> computations)
    {
        this.source = requireNonNull(source, "source is null");
        this.partitionTypes = partitionTypes;
        this.partitionChannels = partitionChannels;
        this.orderChannels = orderChannels;
        this.orderTypes = orderTypes;
        this.orderSortOrders = orderSortOrders;

        ImmutableList.Builder<ScanComputation> scanComputationBuilder = ImmutableList.builder();
        ImmutableList.Builder<Integer> scanOutputIndexesBuilder = ImmutableList.builder();
        ImmutableList.Builder<PrefixSumDifferenceComputation> prefixSumDifferenceComputationBuilder = ImmutableList.builder();
        ImmutableList.Builder<Integer> prefixSumDifferenceOutputIndexesBuilder = ImmutableList.builder();
        for (int i = 0; i < computations.size(); i++) {
            switch (computations.get(i)) {
                case ScanComputation computation -> {
                    scanComputationBuilder.add(computation);
                    scanOutputIndexesBuilder.add(i);
                }
                case PrefixSumDifferenceComputation computation -> {
                    prefixSumDifferenceComputationBuilder.add(computation);
                    prefixSumDifferenceOutputIndexesBuilder.add(i);
                }
            }
        }
        this.scanComputations = scanComputationBuilder.build();
        this.scanOutputIndexes = Ints.toArray(scanOutputIndexesBuilder.build());
        List<PrefixSumDifferenceComputation> prefixSumDifferenceComputations = prefixSumDifferenceComputationBuilder.build();
        this.prefixSumDifferenceOutputIndexes = Ints.toArray(prefixSumDifferenceOutputIndexesBuilder.build());
        Map<Integer, Integer> prefixSumIndexByChannel = new LinkedHashMap<>();
        this.prefixSumIndexes = prefixSumDifferenceComputations.stream()
                .mapToInt(computation -> prefixSumIndexByChannel.computeIfAbsent(computation.inputChannel(), _ -> prefixSumIndexByChannel.size()))
                .toArray();
        this.prefixSumChannels = Ints.toArray(prefixSumIndexByChannel.keySet());
        this.frameComputations = groupByFrame(prefixSumDifferenceComputations, prefixSumIndexes);
        this.hasRowsFrame = frameComputations.stream().anyMatch(frameComputation -> frameComputation.frame().type() == ROWS);
        this.hasRangeFrame = frameComputations.stream().anyMatch(frameComputation -> frameComputation.frame().type() == RANGE);

        this.allocated = ClosingRef.own(context.taskMemoryContext().allocate(getClass().getSimpleName(), MemoryAmount.ZERO));
    }

    private static List<FrameComputations> groupByFrame(List<PrefixSumDifferenceComputation> computations, int[] prefixSumIndexes)
    {
        ImmutableListMultimap.Builder<WindowFrame, Integer> computationIndexesByFrame = ImmutableListMultimap.builder();
        for (int i = 0; i < computations.size(); i++) {
            computationIndexesByFrame.put(computations.get(i).frame(), i);
        }
        ImmutableList.Builder<FrameComputations> frameComputations = ImmutableList.builder();
        computationIndexesByFrame.build().asMap().forEach((frame, indexes) -> {
            int[] computationIndexes = Ints.toArray(indexes);
            int[] framePrefixSumIndexes = Arrays.stream(computationIndexes)
                    .map(computationIndex -> prefixSumIndexes[computationIndex])
                    .distinct()
                    .toArray();
            frameComputations.add(new FrameComputations(frame, computationIndexes, framePrefixSumIndexes));
        });
        return frameComputations.build();
    }

    @Override
    public @Move Result execute()
    {
        if (finished) {
            return new Finished();
        }

        @Own Result sourceResult = source.execute();
        return switch (sourceResult) {
            case Blocked blocked -> blocked;
            case Yielded yielded -> yielded;
            case Data(AllocatedMemory allocation, GpuPage page) -> {
                try (allocation; page) {
                    allocated.borrow().transferFrom(allocation);
                    totalBufferedRowCount += page.positionCount();
                    if (page.columnCount() > 0) {
                        bufferedTables.add(toTable(page));
                    }
                }
                yield new Yielded();
            }
            case Finished() -> {
                finished = true;
                yield computeAndEmit();
            }
        };
    }

    private @Move Result computeAndEmit()
    {
        checkState(totalBufferedRowCount <= Integer.MAX_VALUE, "Buffered row count exceeds int range: %s", totalBufferedRowCount);
        int rowCount = toIntExact(totalBufferedRowCount);
        if (rowCount == 0) {
            return new Finished();
        }

        try (var closer = UncheckedCloser.create()) {
            Optional<@Own Table> sorted = Optional.empty();
            if (!bufferedTables.isEmpty()) {
                sorted = Optional.of(sortBufferedInput());
                closer.register(sorted.get()::close);
                allocated.borrow().update(MemoryAmount.gpuDevice(sorted.get().getDeviceMemorySize()));
            }

            long sortedBytes = sorted.map(Table::getDeviceMemorySize).orElse(0L);
            allocated.borrow().update(MemoryAmount.gpuDevice(sortedBytes + estimateComputePeakBytes(sorted, rowCount)));

            @Own ColumnVector[] resultColumns = computeResultColumns(sorted, rowCount);
            for (ColumnVector column : resultColumns) {
                closer.register(column::close);
            }

            long resultBytes = Arrays.stream(resultColumns).mapToLong(ColumnVector::getDeviceMemorySize).sum();
            allocated.borrow().update(MemoryAmount.gpuDevice(sortedBytes + resultBytes));

            try (ClosingRef<GpuPage> result = ClosingRef.own(buildOutputPage(sorted, resultColumns, rowCount))) {
                allocated.borrow().update(result.borrow().retainedMemory());
                return new Data(allocated.take(), result.take());
            }
        }
    }

    private @Move Table sortBufferedInput()
    {
        checkState(!bufferedTables.isEmpty(), "No buffered tables to sort");

        int partitionCount = partitionChannels.length;
        int orderCount = orderChannels.length;
        int totalSortKeys = partitionCount + orderCount;

        MemoryAmount concatenateMemory = bufferedTables.concatenateMemoryRequirements();
        long currentBytes = allocated.borrow().amount().gpuDeviceBytes();
        if (totalSortKeys == 0) {
            allocated.borrow().update(concatenateMemory);
            return bufferedTables.concatenateAndClear();
        }

        int[] sortChannels = new int[totalSortKeys];
        ImmutableList.Builder<SortOrder> sortOrdersBuilder = ImmutableList.builder();
        // cuDF expects input sorted by the partition channels, then the order channels.
        // ASC_NULLS_LAST must match sortedPartitionGroupByOptions (keysDescending=false, keysNullSmallest=false).
        for (int i = 0; i < partitionCount; i++) {
            sortChannels[i] = partitionChannels[i];
            sortOrdersBuilder.add(SortOrder.ASC_NULLS_LAST);
        }
        for (int i = 0; i < orderCount; i++) {
            sortChannels[partitionCount + i] = orderChannels[i];
            sortOrdersBuilder.add(orderSortOrders.get(i));
        }
        List<SortOrder> sortOrders = sortOrdersBuilder.build();
        List<Type> sortTypes = Stream.concat(partitionTypes.stream(), orderTypes.stream()).collect(toImmutableList());

        MemoryAmount peakMemory = MemoryAmount.max(
                estimateSortMemoryRequirements(sortChannels, sortTypes, sortOrders, currentBytes),
                concatenateMemory);
        allocated.borrow().update(peakMemory);

        try (Table concatenated = bufferedTables.concatenateAndClear()) {
            return orderBy(concatenated, sortChannels, sortOrders, sortTypes);
        }
    }

    private MemoryAmount estimateSortMemoryRequirements(int[] sortChannels, List<Type> sortTypes, List<SortOrder> sortOrders, long currentBytes)
    {
        DType[] sortDTypes = getSortDTypes(bufferedTables.borrow().getFirst(), sortChannels);
        long sortKeyBytes = estimateBufferedSortKeyBytes(sortChannels);
        long sortWorkBytes = estimateOrderByMemoryUsage(sortDTypes, sortTypes, sortOrders, sortKeyBytes, currentBytes, totalBufferedRowCount);
        return MemoryAmount.gpuDevice(sortWorkBytes);
    }

    private static DType[] getSortDTypes(@Borrow Table table, int[] sortChannels)
    {
        return Arrays.stream(sortChannels)
                .mapToObj(channel -> table.getColumn(channel).getType())
                .toArray(DType[]::new);
    }

    private long estimateBufferedSortKeyBytes(int[] sortChannels)
    {
        long bytes = 0;
        for (int i = 0; i < sortChannels.length; i++) {
            int channel = sortChannels[i];
            for (Table table : bufferedTables.borrow()) {
                bytes = addExact(bytes, table.getColumn(channel).getDeviceMemorySize());
            }
        }
        return bytes;
    }

    private long estimateComputePeakBytes(Optional<@Borrow Table> sorted, int rowCount)
    {
        if (scanComputations.isEmpty() && prefixSumDifferenceOutputIndexes.length == 0) {
            return 0;
        }

        long scanOutputBytes = scanComputations.stream()
                .mapToLong(computation -> (long) rowCount * computation.scanOutputBytesPerRow())
                .sum();

        checkState(sorted.isPresent(), "computation requires an input table");
        // The scan batch runs before the prefix sum differences, so the scan outputs stay live throughout them
        return scanOutputBytes + max(
                estimateScanAggregationWorkingBytes(sorted.get(), rowCount),
                estimatePrefixSumDifferencePeakBytes(sorted.get(), rowCount));
    }

    private long estimateScanAggregationWorkingBytes(@Borrow Table sorted, int rowCount)
    {
        if (scanComputations.isEmpty() || partitionChannels.length == 0) {
            return 0;
        }
        long partitionKeyBytes = estimatePartitionKeyBytes(sorted);
        long scanWorkingBytes = scanComputations.stream()
                .mapToLong(computation -> (long) rowCount * computation.groupedScanWorkingBytesPerRow())
                .max()
                .orElse(0);
        return partitionKeyBytes + scanWorkingBytes;
    }

    // On top of the sorted input and the scan outputs, which stay live throughout
    private long estimatePrefixSumDifferencePeakBytes(@Borrow Table sorted, int rowCount)
    {
        if (prefixSumDifferenceOutputIndexes.length == 0) {
            return 0;
        }
        return max(estimateWindowLayoutPeakBytes(sorted, rowCount), estimatePrefixSumDifferenceWorkingBytes(sorted, rowCount));
    }

    private long estimateWindowLayoutPeakBytes(@Borrow Table sorted, int rowCount)
    {
        long int32ColumnBytes = (long) rowCount * INT32_BYTES;
        // The row index, if the ROWS frames need it, is live from the start
        long rowIndexBytes = hasRowsFrame ? int32ColumnBytes : 0;
        // The grouped scan takes the row index and the sorted input as they are, since its keys are sorted, and returns
        // a copy of the partition keys along with the partition start
        long partitionStartPeakBytes = rowIndexBytes + int32ColumnBytes;
        if (partitionChannels.length > 0) {
            long sortHelperBytes = (long) rowCount * GROUPED_SCAN_SORT_HELPER_BYTES_PER_ROW;
            partitionStartPeakBytes = int32ColumnBytes + sortHelperBytes + estimatePartitionKeyBytes(sorted) + int32ColumnBytes;
        }
        long peakBytes = partitionStartPeakBytes;

        // The partition start, and for ROWS frames the row index and the partition end, stay live from here on
        long liveBytes = int32ColumnBytes;
        if (hasRowsFrame) {
            liveBytes += 2 * int32ColumnBytes;
            peakBytes = max(peakBytes, liveBytes);
        }

        // The search for the peer group end keeps the NaN sort keys live
        if (hasRangeFrame) {
            long nanSortKeyBytes = 0;
            for (int i = 0; i < orderChannels.length; i++) {
                if (needsNanSortKey(orderTypes.get(i), orderSortOrders.get(i))) {
                    nanSortKeyBytes += (long) rowCount * BOOL8_BYTES + nullMaskBytes(rowCount);
                }
            }
            peakBytes = max(peakBytes, liveBytes + nanSortKeyBytes + int32ColumnBytes);
        }
        return peakBytes;
    }

    private long estimateWindowLayoutBytes(int rowCount)
    {
        int columnCount = 1; // partition start
        if (hasRowsFrame) {
            columnCount += 2; // row index and partition end
        }
        if (hasRangeFrame) {
            columnCount += 1; // peer group end
        }
        return (long) columnCount * rowCount * INT32_BYTES;
    }

    // The frames are processed one at a time, and the frame sums of each stay live until the end. The window layout and
    // the prefix sums are live throughout, so the peak is when the frame bounds and the gathers of the prefix sums of
    // a frame are live, or when its frame bounds are computed. Building the prefix sums takes less: two more copies of
    // one input column at most.
    private long estimatePrefixSumDifferenceWorkingBytes(@Borrow Table sorted, int rowCount)
    {
        long prefixSumBytes = 0;
        for (int channel : prefixSumChannels) {
            int valueBytes = sorted.getColumn(channel).getType().getSizeInBytes();
            prefixSumBytes += ((long) rowCount + 1) * valueBytes;
        }

        long allFrameSumBytes = 0;
        long framePeakBytes = 0;
        for (FrameComputations computations : frameComputations) {
            long frameSumBytes = 0;
            for (int prefixSumIndex : computations.prefixSumIndexes()) {
                int valueBytes = sorted.getColumn(prefixSumChannels[prefixSumIndex]).getType().getSizeInBytes();
                frameSumBytes += estimatePrefixSumDifferenceBytes(valueBytes, rowCount);
            }
            allFrameSumBytes += frameSumBytes;
            // Bounds computed from an offset come out of binary operations, which may allocate validity masks
            long frameBoundsBytes = 2 * ((long) rowCount * INT32_BYTES + nullMaskBytes(rowCount));
            framePeakBytes = max(framePeakBytes, max(
                    estimateFrameBoundsPeakBytes(computations.frame(), rowCount),
                    frameBoundsBytes + 2 * frameSumBytes));
        }
        return estimateWindowLayoutBytes(rowCount) + prefixSumBytes + allFrameSumBytes + framePeakBytes;
    }

    private static long estimateFrameBoundsPeakBytes(WindowFrame frame, int rowCount)
    {
        if (!frame.hasOffset()) {
            // Both bounds, of which the end of a frame ending at the current row is a new column
            return 2L * rowCount * INT32_BYTES;
        }
        // An offset bound computed after the other bound: the other bound, the INT32 base row index, the clamped offset,
        // the INT64 bound before and after clamping to the partition, and its cast to INT32. All but the base row index
        // come out of binary operations or casts of their results, which may allocate validity masks.
        return rowCount * (3L * INT32_BYTES + 3L * INT64_BYTES) + 5 * nullMaskBytes(rowCount);
    }

    // A gather with NULLIFY always allocates a validity mask, so the gathered prefix sums and their difference have one
    private static long estimatePrefixSumDifferenceBytes(int valueBytes, int rowCount)
    {
        return (long) rowCount * valueBytes + nullMaskBytes(rowCount);
    }

    private long estimatePartitionKeyBytes(@Borrow Table sorted)
    {
        long bytes = 0;
        for (int channel : partitionChannels) {
            bytes += sorted.getColumn(channel).getDeviceMemorySize();
        }
        return bytes;
    }

    private @Move ColumnVector[] computeResultColumns(Optional<@Borrow Table> sorted, int rowCount)
    {
        if (scanComputations.isEmpty() && prefixSumDifferenceOutputIndexes.length == 0) {
            return new ColumnVector[0];
        }
        checkState(sorted.isPresent(), "input table is required");

        @Own ColumnVector[] results = new ColumnVector[scanComputations.size() + prefixSumDifferenceOutputIndexes.length];
        try {
            @Own ColumnVector[] scanResults = computeScanAggregations(sorted.get());
            for (int i = 0; i < scanResults.length; i++) {
                results[scanOutputIndexes[i]] = scanResults[i];
            }
            @Own ColumnVector[] prefixSumDifferenceResults = computePrefixSumDifferences(sorted.get(), rowCount);
            for (int i = 0; i < prefixSumDifferenceResults.length; i++) {
                results[prefixSumDifferenceOutputIndexes[i]] = prefixSumDifferenceResults[i];
            }
            return results;
        }
        catch (Throwable t) {
            closeColumns(results);
            throw t;
        }
    }

    private @Move ColumnVector[] computeScanAggregations(@Borrow Table sorted)
    {
        if (scanComputations.isEmpty()) {
            return new ColumnVector[0];
        }
        if (partitionChannels.length > 0) {
            return computePartitionedScanColumns(sorted);
        }
        return computeUnpartitionedScanColumns(sorted);
    }

    private @Move ColumnVector[] computeUnpartitionedScanColumns(@Borrow Table sorted)
    {
        @Own ColumnVector[] results = new ColumnVector[scanComputations.size()];
        try {
            for (int i = 0; i < scanComputations.size(); i++) {
                ScanComputation computation = scanComputations.get(i);
                results[i] = sorted.getColumn(computation.inputChannel()).scan(computation.scanAggregation());
            }
            return results;
        }
        catch (Throwable t) {
            closeColumns(results);
            throw t;
        }
    }

    private @Move ColumnVector[] computePartitionedScanColumns(@Borrow Table sorted)
    {
        @Own ColumnVector[] results = new ColumnVector[scanComputations.size()];
        try {
            int[] scanInputChannels = scanComputations.stream()
                    .mapToInt(ScanComputation::inputChannel)
                    .toArray();
            GroupByScanAggregation[] aggregations = scanComputations.stream()
                    .map(ScanComputation::groupedScanAggregation)
                    .toArray(GroupByScanAggregation[]::new);
            try (Table groupedResult = groupedScan(sorted, scanInputChannels, partitionChannels, aggregations)) {
                for (int i = 0; i < scanComputations.size(); i++) {
                    results[i] = groupedResult.getColumn(partitionChannels.length + i).incRefCount();
                }
                return results;
            }
        }
        catch (Throwable t) {
            closeColumns(results);
            throw t;
        }
    }

    // With P the prefix sums over the whole input, preceded by a zero, the sum over a frame is P[endExclusive] -
    // P[startInclusive]. The frame bounds never leave the partition of the row, so the prefix sums need not restart at
    // partition boundaries.
    private @Move ColumnVector[] computePrefixSumDifferences(@Borrow Table sorted, int rowCount)
    {
        if (prefixSumDifferenceOutputIndexes.length == 0) {
            return new ColumnVector[0];
        }

        @Own ColumnVector[] results = new ColumnVector[prefixSumDifferenceOutputIndexes.length];
        try (WindowLayout layout = createWindowLayout(sorted, rowCount);
                Table prefixSums = prefixSums(sorted, prefixSumChannels)) {
            for (FrameComputations computations : frameComputations) {
                int[] framePrefixSumIndexes = computations.prefixSumIndexes();
                @Own ColumnVector[] prefixSumDifferences = new ColumnVector[framePrefixSumIndexes.length];
                // Every bound is between 0 and the row count, so none is out of bounds, but NULLIFY keeps any mistake
                // from reading outside the table
                try (FrameBounds bounds = frameBounds(sorted, layout, computations.frame());
                        Table framePrefixSums = selectColumns(prefixSums, framePrefixSumIndexes);
                        Table prefixSumsAtEnd = framePrefixSums.gather(bounds.endExclusive(), OutOfBoundsPolicy.NULLIFY);
                        Table prefixSumsAtStart = framePrefixSums.gather(bounds.startInclusive(), OutOfBoundsPolicy.NULLIFY)) {
                    for (int i = 0; i < framePrefixSumIndexes.length; i++) {
                        @Borrow ColumnVector atStart = prefixSumsAtStart.getColumn(i);
                        @Borrow ColumnVector atEnd = prefixSumsAtEnd.getColumn(i);
                        prefixSumDifferences[i] = atEnd.binaryOp(SUB, atStart, atEnd.getType());
                    }
                    for (int computationIndex : computations.computationIndexes()) {
                        int differenceIndex = Ints.indexOf(framePrefixSumIndexes, prefixSumIndexes[computationIndex]);
                        results[computationIndex] = prefixSumDifferences[differenceIndex].incRefCount();
                    }
                }
                finally {
                    closeColumns(prefixSumDifferences);
                }
            }
            return results;
        }
        catch (Throwable t) {
            closeColumns(results);
            throw t;
        }
    }

    private static @Move Table prefixSums(@Borrow Table sorted, int[] channels)
    {
        @Own ColumnVector[] columns = new ColumnVector[channels.length];
        try {
            for (int i = 0; i < channels.length; i++) {
                @Borrow ColumnVector input = sorted.getColumn(channels[i]);
                try (Scalar zero = zero(input.getType());
                        ColumnVector nonNullInput = input.replaceNulls(zero);
                        // cuDF sum scans wrap around on overflow instead of failing, but no prefix sum here can overflow: a cuDF column holds
                        // at most 2^31 - 1 rows, so the prefix sums are bounded by 2^31 - 1 for counts, by 10^28 * (2^31 - 1) < 2^127 for
                        // DECIMAL128 values of precision at most 28, and by (2^31 - 1) * (2^32 - 1) < 2^63 for 32-bit chunks widened to INT64.
                        ColumnVector scan = nonNullInput.scan(ScanAggregation.sum());
                        ColumnVector leadingZero = ColumnVector.fromScalar(zero, 1)) {
                    columns[i] = ColumnVector.concatenate(leadingZero, scan);
                }
            }
            return new Table(columns);
        }
        finally {
            closeColumns(columns);
        }
    }

    private static @Move Scalar zero(DType type)
    {
        if (type.equals(DType.INT64)) {
            return Scalar.fromLong(0);
        }
        checkState(type.getTypeId() == DType.DTypeEnum.DECIMAL128, "Unsupported frame sum input type: %s", type);
        return Scalar.fromDecimal(BigInteger.ZERO, type);
    }

    // For each row, the INT32 row indexes bounding its frame. Both are within the partition of the row, and the end never
    // precedes the start, so the two are equal for an empty frame.
    private record FrameBounds(@Own ColumnVector startInclusive, @Own ColumnVector endExclusive)
            implements AutoCloseable
    {
        public FrameBounds
        {
            requireNonNull(startInclusive, "startInclusive is null");
            requireNonNull(endExclusive, "endExclusive is null");
        }

        @Override
        public void close()
        {
            closeColumns(startInclusive, endExclusive);
        }
    }

    private static @Move FrameBounds frameBounds(@Borrow Table sorted, @Borrow WindowLayout layout, WindowFrame frame)
    {
        try (ClosingRef<ColumnVector> startInclusive = ClosingRef.own(frameBound(sorted, layout, frame.type(), frame.start(), BoundSide.START_INCLUSIVE));
                ClosingRef<ColumnVector> endExclusive = ClosingRef.own(frameBound(sorted, layout, frame.type(), frame.end(), BoundSide.END_EXCLUSIVE))) {
            if (frame.hasOffset()) {
                // An offset bound can make the frame end before it starts, e.g. ROWS BETWEEN 1 PRECEDING AND 2 PRECEDING,
                // or past the partition, e.g. ROWS BETWEEN 2 FOLLOWING AND 1 FOLLOWING. Such frames are empty.
                endExclusive.replace(startInclusive.borrow().binaryOp(NULL_MAX, endExclusive.borrow(), DType.INT32));
            }
            return new FrameBounds(startInclusive.take(), endExclusive.take());
        }
    }

    private enum BoundSide
    {
        START_INCLUSIVE,
        END_EXCLUSIVE,
    }

    private static @Move ColumnVector frameBound(@Borrow Table sorted, @Borrow WindowLayout layout, WindowFrameType frameType, FrameBound bound, BoundSide side)
    {
        return switch (bound.type()) {
            case UNBOUNDED_PRECEDING -> {
                checkArgument(side == BoundSide.START_INCLUSIVE, "Frame cannot end at UNBOUNDED PRECEDING");
                yield layout.partitionStartInclusive().incRefCount();
            }
            case UNBOUNDED_FOLLOWING -> {
                checkArgument(side == BoundSide.END_EXCLUSIVE, "Frame cannot start at UNBOUNDED FOLLOWING");
                yield layout.partitionEndExclusive().orElseThrow().incRefCount();
            }
            case CURRENT_ROW -> switch (frameType) {
                case ROWS -> currentRowBound(layout, side);
                case RANGE -> {
                    checkArgument(side == BoundSide.END_EXCLUSIVE, "RANGE frame starting at CURRENT ROW is not supported");
                    yield layout.peerGroupEndExclusive().orElseThrow().incRefCount();
                }
                case GROUPS -> throw new IllegalArgumentException("GROUPS frame is not supported");
            };
            case PRECEDING, FOLLOWING -> {
                checkArgument(frameType == ROWS, "Offset bound of a %s frame is not supported", frameType);
                yield rowsOffsetBound(sorted, layout, bound, side);
            }
        };
    }

    // The frame starts k rows before or after the current row, and ends k rows before or after the row after it, but
    // neither leaves the partition
    private static @Move ColumnVector rowsOffsetBound(@Borrow Table sorted, @Borrow WindowLayout layout, FrameBound bound, BoundSide side)
    {
        @Borrow ColumnVector offset = sorted.getColumn(bound.offsetChannel().orElseThrow());
        checkState(offset.getType().equals(DType.INT64), "Unsupported frame offset type: %s", offset.getType());

        try (ColumnVector rowIndex = currentRowBound(layout, side)) {
            return switch (bound.type()) {
                case PRECEDING -> {
                    // The subtraction runs in INT64, and the planner fails the query for a negative offset, so it cannot overflow
                    try (ColumnVector unclamped = rowIndex.binaryOp(SUB, offset, DType.INT64);
                            ColumnVector clamped = unclamped.binaryOp(NULL_MAX, layout.partitionStartInclusive(), DType.INT64)) {
                        yield clamped.castTo(DType.INT32);
                    }
                }
                case FOLLOWING -> {
                    // An offset can be as large as Long.MAX_VALUE, so adding it to the row index could overflow INT64. No frame
                    // reaches more than rowCount rows past the current row, so clamping the offset to rowCount gives the same
                    // frame, and the sum, at most twice rowCount, fits.
                    try (Scalar rowCount = Scalar.fromLong(layout.rowCount());
                            ColumnVector clampedOffset = offset.binaryOp(NULL_MIN, rowCount, DType.INT64);
                            ColumnVector unclamped = rowIndex.binaryOp(ADD, clampedOffset, DType.INT64);
                            ColumnVector clamped = unclamped.binaryOp(NULL_MIN, layout.partitionEndExclusive().orElseThrow(), DType.INT64)) {
                        yield clamped.castTo(DType.INT32);
                    }
                }
                case UNBOUNDED_PRECEDING, CURRENT_ROW, UNBOUNDED_FOLLOWING -> throw new IllegalArgumentException("Not an offset bound: " + bound);
            };
        }
    }

    // The bound at the current row: the row index for the start, and the next row index for the end
    private static @Move ColumnVector currentRowBound(@Borrow WindowLayout layout, BoundSide side)
    {
        return switch (side) {
            case START_INCLUSIVE -> layout.rowIndex().orElseThrow().incRefCount();
            case END_EXCLUSIVE -> nextRowIndex(layout);
        };
    }

    private static @Move ColumnVector nextRowIndex(@Borrow WindowLayout layout)
    {
        try (Scalar one = Scalar.fromInt(1)) {
            return layout.rowIndex().orElseThrow().binaryOp(ADD, one, DType.INT32);
        }
    }

    // How the sorted input divides into partitions and peer groups, from which the frame bounds are resolved. Starts and
    // ends are INT32 row indexes. The row index and the partition end are present only for ROWS frames, and the peer
    // group end only for RANGE frames.
    private record WindowLayout(
            int rowCount,
            Optional<@Own ColumnVector> rowIndex,
            @Own ColumnVector partitionStartInclusive,
            Optional<@Own ColumnVector> partitionEndExclusive,
            Optional<@Own ColumnVector> peerGroupEndExclusive)
            implements AutoCloseable
    {
        public WindowLayout
        {
            requireNonNull(rowIndex, "rowIndex is null");
            requireNonNull(partitionStartInclusive, "partitionStartInclusive is null");
            requireNonNull(partitionEndExclusive, "partitionEndExclusive is null");
            requireNonNull(peerGroupEndExclusive, "peerGroupEndExclusive is null");
        }

        @Override
        public void close()
        {
            closeColumns(rowIndex.orElse(null), partitionStartInclusive, partitionEndExclusive.orElse(null), peerGroupEndExclusive.orElse(null));
        }
    }

    private @Move WindowLayout createWindowLayout(@Borrow Table sorted, int rowCount)
    {
        try (Scalar zero = Scalar.fromInt(0);
                ClosingRef<ColumnVector> rowIndex = ClosingRef.empty();
                ClosingRef<ColumnVector> partitionStartInclusive = ClosingRef.empty();
                ClosingRef<ColumnVector> partitionEndExclusive = ClosingRef.empty();
                ClosingRef<ColumnVector> peerGroupEndExclusive = ClosingRef.empty()) {
            if (hasRowsFrame) {
                rowIndex.set(ColumnVector.sequence(zero, rowCount));
            }
            if (partitionChannels.length == 0) {
                partitionStartInclusive.set(ColumnVector.fromScalar(zero, rowCount));
            }
            else if (hasRowsFrame) {
                partitionStartInclusive.set(partitionStart(sorted, rowIndex.borrow()));
            }
            else {
                try (ColumnVector partitionRowIndex = ColumnVector.sequence(zero, rowCount)) {
                    partitionStartInclusive.set(partitionStart(sorted, partitionRowIndex));
                }
            }
            if (hasRowsFrame) {
                partitionEndExclusive.set(partitionEnd(partitionStartInclusive.borrow(), rowCount));
            }
            if (hasRangeFrame) {
                peerGroupEndExclusive.set(peerGroupEnd(sorted, partitionStartInclusive.borrow()));
            }
            return new WindowLayout(
                    rowCount,
                    rowIndex.takeIfPresent(),
                    partitionStartInclusive.take(),
                    partitionEndExclusive.takeIfPresent(),
                    peerGroupEndExclusive.takeIfPresent());
        }
    }

    // The running minimum of the row index within a partition is the index of the first row of the partition
    private @Move ColumnVector partitionStart(@Borrow Table sorted, @Borrow ColumnVector rowIndex)
    {
        int[] rowIndexChannel = new int[] {sorted.getNumberOfColumns()}; // we append the row index right after the last input table column
        try (Table scanInput = appendColumns(sorted, new ColumnVector[] {rowIndex});
                Table groupedResult = groupedScan(scanInput, rowIndexChannel, partitionChannels, new GroupByScanAggregation[] {GroupByScanAggregation.min()})) {
            return groupedResult.getColumn(partitionChannels.length).incRefCount();
        }
    }

    // For each row, the INT32 index of the first row after its partition
    private @Move ColumnVector partitionEnd(@Borrow ColumnVector partitionStart, int rowCount)
    {
        if (partitionChannels.length == 0) {
            try (Scalar rowCountScalar = Scalar.fromInt(rowCount)) {
                return ColumnVector.fromScalar(rowCountScalar, rowCount);
            }
        }
        // The partition start is ascending, so the end of the partition of a row is the upper bound of its partition start
        try (Table partitionStarts = new Table(partitionStart)) {
            boolean[] nullsSmallest = new boolean[] {false}; // there are no nulls in the partition start
            boolean[] descending = new boolean[] {false};
            return partitionStarts.upperBound(nullsSmallest, partitionStarts, descending);
        }
    }

    // For each row, the INT32 index of the first row after its peer group.
    private @Move ColumnVector peerGroupEnd(@Borrow Table sorted, @Borrow ColumnVector partitionStart)
    {
        // The input is sorted, so the end of the peer group of a row is the upper bound of its own keys. The search
        // compares the keys as orderBy sorted them, with the same NaN sort keys first.
        @Borrow List<ColumnVector> keys = new ArrayList<>();
        List<Boolean> nullsSmallest = new ArrayList<>();
        List<Boolean> descending = new ArrayList<>();
        @Own List<ColumnVector> nanSortKeys = new ArrayList<>();
        try {
            keys.add(partitionStart);
            nullsSmallest.add(false);
            descending.add(false);
            for (int i = 0; i < orderChannels.length; i++) {
                @Borrow ColumnVector key = sorted.getColumn(orderChannels[i]);
                SortOrder sortOrder = orderSortOrders.get(i);
                if (needsNanSortKey(orderTypes.get(i), sortOrder)) {
                    @Own ColumnVector nanSortKey = createNanSortKey(key, sortOrder.isAscending());
                    nanSortKeys.add(nanSortKey);
                    keys.add(nanSortKey);
                    nullsSmallest.add(isNullSmallest(SortOrder.ASC_NULLS_FIRST));
                    descending.add(false);
                }
                keys.add(key);
                nullsSmallest.add(isNullSmallest(sortOrder));
                descending.add(!sortOrder.isAscending());
            }
            try (Table peerKeys = new Table(keys.toArray(ColumnVector[]::new))) {
                return peerKeys.upperBound(Booleans.toArray(nullsSmallest), peerKeys, Booleans.toArray(descending));
            }
        }
        finally {
            closeColumns(nanSortKeys.toArray(ColumnVector[]::new));
        }
    }

    // Output layout: [sorted columns 0..N-1, result columns N..N+S-1].
    // GpuWindowCompiler.buildPipeline relies on this to build post-projections.
    private static @Move GpuPage buildOutputPage(Optional<@Borrow Table> sorted, @Borrow ColumnVector[] resultColumns, int rowCount)
    {
        if (sorted.isEmpty()) {
            return toGpuPageFromColumns(rowCount, resultColumns);
        }
        if (resultColumns.length == 0) {
            return toGpuPage(sorted.get());
        }
        try (Table outputTable = appendColumns(sorted.get(), resultColumns)) {
            return toGpuPage(outputTable);
        }
    }

    private static @Move GpuPage toGpuPageFromColumns(int rowCount, @Borrow ColumnVector[] columns)
    {
        @Own Column[] outputColumns = new Column[columns.length];
        try {
            for (int i = 0; i < columns.length; i++) {
                outputColumns[i] = new DeviceMemory(columns[i].incRefCount());
            }
            return new GpuPage(rowCount, outputColumns);
        }
        finally {
            closeColumns(outputColumns);
        }
    }

    private static @Move Table appendColumns(@Borrow Table table, @Borrow ColumnVector[] toAppend)
    {
        int sourceColumnCount = table.getNumberOfColumns();
        int totalColumns = sourceColumnCount + toAppend.length;
        @Own ColumnVector[] allColumns = new ColumnVector[totalColumns];
        try {
            for (int i = 0; i < sourceColumnCount; i++) {
                allColumns[i] = table.getColumn(i).incRefCount();
            }
            for (int i = 0; i < toAppend.length; i++) {
                allColumns[i + sourceColumnCount] = toAppend[i].incRefCount();
            }
            return new Table(allColumns);
        }
        finally {
            closeColumns(allColumns);
        }
    }

    private static @Move Table groupedScan(@Borrow Table sorted, int[] scanInputChannels, int[] partitionChannels, GroupByScanAggregation[] aggregations)
    {
        checkArgument(aggregations.length > 0, "aggregations is empty");
        checkArgument(scanInputChannels.length == aggregations.length, "scanInputChannels and aggregations sizes do not match");

        GroupByScanAggregationOnColumn[] aggregationColumns = new GroupByScanAggregationOnColumn[aggregations.length];
        for (int i = 0; i < aggregations.length; i++) {
            int scanInputChannel = scanInputChannels[i];
            checkArgument(scanInputChannel >= 0 && scanInputChannel < sorted.getNumberOfColumns(), "scanInputChannel is out of range");
            aggregationColumns[i] = aggregations[i].onColumn(scanInputChannel);
        }

        return sorted.groupBy(sortedPartitionGroupByOptions(partitionChannels), partitionKeyIndices(partitionChannels))
                .scan(aggregationColumns);
    }

    private static GroupByOptions sortedPartitionGroupByOptions(int[] partitionChannels)
    {
        int partitionCount = partitionChannels.length;
        boolean[] keysDescending = new boolean[partitionCount];
        boolean[] keysNullSmallest = new boolean[partitionCount];
        return GroupByOptions.builder()
                .withIgnoreNullKeys(false)
                .withKeysSorted(true)
                .withKeysDescending(keysDescending)
                .withKeysNullSmallest(keysNullSmallest)
                .build();
    }

    private static int[] partitionKeyIndices(int[] partitionChannels)
    {
        int partitionCount = partitionChannels.length;
        int[] indices = new int[partitionCount];
        System.arraycopy(partitionChannels, 0, indices, 0, partitionCount);
        return indices;
    }

    @Override
    public void close()
    {
        try (var closer = UncheckedCloser.create()) {
            closer.register(source);
            closer.register(allocated);
            closer.register(bufferedTables);
        }
    }
}
