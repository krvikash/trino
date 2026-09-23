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
import ai.rapids.cudf.ScanAggregation;
import ai.rapids.cudf.Table;
import com.google.common.collect.ImmutableList;
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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.gpu.GpuSortUtils.estimateOrderByMemoryUsage;
import static io.trino.operator.gpu.GpuSortUtils.orderBy;
import static io.trino.plugin.base.gpu.GpuUtils.closeColumns;
import static io.trino.plugin.base.gpu.GpuUtils.toGpuPage;
import static io.trino.plugin.base.gpu.GpuUtils.toTable;
import static java.lang.Math.addExact;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class GpuWindow
        implements GpuOperation
{
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
        for (WindowComputation windowComputationOnChannel : computations) {
            switch (windowComputationOnChannel) {
                case ScanComputation computation -> scanComputationBuilder.add(computation);
            }
        }
        this.scanComputations = scanComputationBuilder.build();

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

            @Own ColumnVector[] resultColumns = computeResultColumns(sorted);
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

    private @Move ColumnVector[] computeResultColumns(Optional<@Borrow Table> sorted)
    {
        if (scanComputations.isEmpty()) {
            return new ColumnVector[0];
        }
        checkState(sorted.isPresent(), "input table is required");
        if (partitionChannels.length > 0) {
            return computePartitionedScanColumns(sorted.get());
        }
        return computeUnpartitionedScanColumns(sorted.get());
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

    private long estimateComputePeakBytes(Optional<@Borrow Table> sorted, int rowCount)
    {
        if (scanComputations.isEmpty()) {
            return 0;
        }

        long scanOutputBytes = scanComputations.stream()
                .mapToLong(computation -> (long) rowCount * computation.scanOutputBytesPerRow())
                .sum();
        if (partitionChannels.length == 0) {
            return scanOutputBytes;
        }

        checkState(sorted.isPresent(), "computation requires an input table");
        long partitionKeyBytes = estimatePartitionKeyBytes(sorted.get());
        long scanWorkingBytes = scanComputations.stream()
                .mapToLong(computation -> (long) rowCount * computation.groupedScanWorkingBytesPerRow())
                .max()
                .orElse(0);
        return partitionKeyBytes + scanOutputBytes + scanWorkingBytes;
    }

    private long estimatePartitionKeyBytes(@Borrow Table sorted)
    {
        long bytes = 0;
        for (int channel : partitionChannels) {
            bytes += sorted.getColumn(channel).getDeviceMemorySize();
        }
        return bytes;
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
