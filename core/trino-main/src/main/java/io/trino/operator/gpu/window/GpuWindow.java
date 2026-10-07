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
package io.trino.operator.gpu.window;

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
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Ints;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.memory.AllocatedMemory;
import io.trino.plugin.base.gpu.ClosingRef;
import io.trino.plugin.base.gpu.TablesList;
import io.trino.plugin.base.gpu.UncheckedCloser;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.MemoryAmount;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static ai.rapids.cudf.BinaryOp.SUB;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.GpuSortUtils.createNanSortKey;
import static io.trino.operator.gpu.GpuSortUtils.estimateOrderByMemoryUsage;
import static io.trino.operator.gpu.GpuSortUtils.isNullSmallest;
import static io.trino.operator.gpu.GpuSortUtils.needsNanSortKey;
import static io.trino.operator.gpu.GpuSortUtils.orderBy;
import static io.trino.operator.gpu.memory.GpuMemoryUtils.nullMaskBytes;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.plugin.base.gpu.GpuUtils.toGpuPage;
import static io.trino.plugin.base.gpu.GpuUtils.toTable;
import static java.lang.Math.addExact;
import static java.lang.Math.max;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class GpuWindow
        implements GpuOperation
{
    private static final int INT32_BYTES = DType.INT32.getSizeInBytes();
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
    private final List<PrefixSumDifferenceComputation> prefixSumDifferenceComputations;
    private final int[] prefixSumDifferenceOutputIndexes;
    // The distinct input channels of the frame sums, and the index into them of the input channel of each frame sum
    private final int[] prefixSumChannels;
    private final int[] prefixSumIndexes;

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

    public record PrefixSumDifferenceComputation(int inputChannel)
            implements WindowComputation
    {
        public PrefixSumDifferenceComputation
        {
            checkArgument(inputChannel >= 0, "inputChannel is negative");
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
        this.prefixSumDifferenceComputations = prefixSumDifferenceComputationBuilder.build();
        this.prefixSumDifferenceOutputIndexes = Ints.toArray(prefixSumDifferenceOutputIndexesBuilder.build());
        Map<Integer, Integer> prefixSumIndexByChannel = new LinkedHashMap<>();
        this.prefixSumIndexes = prefixSumDifferenceComputations.stream()
                .mapToInt(computation -> prefixSumIndexByChannel.computeIfAbsent(computation.inputChannel(), _ -> prefixSumIndexByChannel.size()))
                .toArray();
        this.prefixSumChannels = Ints.toArray(prefixSumIndexByChannel.keySet());

        this.allocated = ClosingRef.own(context.taskMemoryContext().allocate(getClass().getSimpleName(), MemoryAmount.ZERO));
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
        if (scanComputations.isEmpty() && prefixSumDifferenceComputations.isEmpty()) {
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
        if (prefixSumDifferenceComputations.isEmpty()) {
            return 0;
        }
        return max(estimateWindowLayoutPeakBytes(sorted, rowCount), estimatePrefixSumDifferenceWorkingBytes(sorted, rowCount));
    }

    private long estimateWindowLayoutPeakBytes(@Borrow Table sorted, int rowCount)
    {
        long partitionStartBytes = (long) rowCount * INT32_BYTES;
        // The grouped scan takes the row index and the sorted input as they are, since its keys are sorted, and returns
        // a copy of the partition keys along with the partition start
        long partitionStartPeakBytes = partitionStartBytes;
        if (partitionChannels.length > 0) {
            long rowIndexBytes = (long) rowCount * INT32_BYTES;
            long sortHelperBytes = (long) rowCount * GROUPED_SCAN_SORT_HELPER_BYTES_PER_ROW;
            partitionStartPeakBytes = rowIndexBytes + sortHelperBytes + estimatePartitionKeyBytes(sorted) + partitionStartBytes;
        }

        // The search for the peer group end keeps the partition start and the NaN sort keys live
        long nanSortKeyBytes = 0;
        for (int i = 0; i < orderChannels.length; i++) {
            if (needsNanSortKey(orderTypes.get(i), orderSortOrders.get(i))) {
                nanSortKeyBytes += (long) rowCount * BOOL8_BYTES + nullMaskBytes(rowCount);
            }
        }
        long peerGroupEndBytes = (long) rowCount * INT32_BYTES;
        return max(partitionStartPeakBytes, partitionStartBytes + nanSortKeyBytes + peerGroupEndBytes);
    }

    // The window layout, the prefix sums, both of their gathers and the frame sums are live together once the last
    // frame sum is computed. Building the prefix sums takes less: two more copies of one input column at most.
    private long estimatePrefixSumDifferenceWorkingBytes(@Borrow Table sorted, int rowCount)
    {
        long windowLayoutBytes = 2L * rowCount * INT32_BYTES;
        long prefixSumBytes = 0;
        long prefixSumDifferenceBytes = 0;
        for (int channel : prefixSumChannels) {
            int valueBytes = sorted.getColumn(channel).getType().getSizeInBytes();
            prefixSumBytes += ((long) rowCount + 1) * valueBytes;
            prefixSumDifferenceBytes += estimatePrefixSumDifferenceBytes(valueBytes, rowCount);
        }
        return windowLayoutBytes + prefixSumBytes + 3 * prefixSumDifferenceBytes;
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
        if (scanComputations.isEmpty() && prefixSumDifferenceComputations.isEmpty()) {
            return new ColumnVector[0];
        }
        checkState(sorted.isPresent(), "input table is required");

        @Own ColumnVector[] results = new ColumnVector[scanComputations.size() + prefixSumDifferenceComputations.size()];
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
        if (prefixSumDifferenceComputations.isEmpty()) {
            return new ColumnVector[0];
        }

        @Own ColumnVector[] results = new ColumnVector[prefixSumDifferenceComputations.size()];
        try (WindowLayout layout = createWindowLayout(sorted, rowCount);
                Table prefixSums = prefixSums(sorted, prefixSumChannels)) {
            try (FrameBounds bounds = frameBounds(layout)) {
                // Every bound is between 0 and the row count, so none is out of bounds, but NULLIFY keeps any mistake
                // from reading outside the table
                @Own ColumnVector[] prefixSumDifferences = new ColumnVector[prefixSumChannels.length];
                try (Table prefixSumsAtEnd = prefixSums.gather(bounds.endExclusive(), OutOfBoundsPolicy.NULLIFY);
                        Table prefixSumsAtStart = prefixSums.gather(bounds.startInclusive(), OutOfBoundsPolicy.NULLIFY)) {
                    for (int i = 0; i < prefixSumChannels.length; i++) {
                        @Borrow ColumnVector atStart = prefixSumsAtStart.getColumn(i);
                        @Borrow ColumnVector atEnd = prefixSumsAtEnd.getColumn(i);
                        prefixSumDifferences[i] = atEnd.binaryOp(SUB, atStart, atEnd.getType());
                    }
                    for (int i = 0; i < prefixSumDifferenceComputations.size(); i++) {
                        results[i] = prefixSumDifferences[prefixSumIndexes[i]].incRefCount();
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
            closeColumns(new ColumnVector[] {startInclusive, endExclusive});
        }
    }

    private static @Move FrameBounds frameBounds(@Borrow WindowLayout layout)
    {
        // TODO: Support frame definitions other than default: https://starburstdata.atlassian.net/browse/ENG-28121
        // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        return new FrameBounds(layout.partitionStartInclusive().incRefCount(), layout.peerGroupEndExclusive().incRefCount());
    }

    // How the sorted input divides into partitions and peer groups, from which the frame bounds are resolved.
    // Starts and ends are INT32 row indexes.
    private record WindowLayout(
            @Own ColumnVector partitionStartInclusive,
            @Own ColumnVector peerGroupEndExclusive)
            implements AutoCloseable
    {
        public WindowLayout
        {
            requireNonNull(partitionStartInclusive, "partitionStartInclusive is null");
            requireNonNull(peerGroupEndExclusive, "peerGroupEndExclusive is null");
        }

        @Override
        public void close()
        {
            closeColumns(new ColumnVector[] {partitionStartInclusive, peerGroupEndExclusive});
        }
    }

    private @Move WindowLayout createWindowLayout(@Borrow Table sorted, int rowCount)
    {
        try (Scalar zero = Scalar.fromInt(0);
                ClosingRef<ColumnVector> partitionStartInclusive = ClosingRef.empty();
                ClosingRef<ColumnVector> peerGroupEndExclusive = ClosingRef.empty()) {
            if (partitionChannels.length == 0) {
                partitionStartInclusive.set(ColumnVector.fromScalar(zero, rowCount));
            }
            else {
                try (ColumnVector rowIndex = ColumnVector.sequence(zero, rowCount)) {
                    partitionStartInclusive.set(partitionStart(sorted, rowIndex));
                }
            }
            peerGroupEndExclusive.set(peerGroupEnd(sorted, partitionStartInclusive.borrow()));
            return new WindowLayout(
                    partitionStartInclusive.take(),
                    peerGroupEndExclusive.take());
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
