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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimaps;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.getMaxSplitSize;
import static io.trino.plugin.deltalake.DeltaLakeSessionProperties.isCompositeSplitsEnabled;
import static io.trino.plugin.deltalake.util.DeltaLakeDomains.partitionMatchesPredicate;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class DeltaLakeSplitSource
        implements ConnectorSplitSource
{
    private static final Logger LOG = Logger.get(DeltaLakeSplitSource.class);

    private final SchemaTableName tableName;
    private final AsyncQueue<ConnectorSplit> queue;
    private final boolean recordScannedFiles;
    private final boolean compositeSplitsEnabled;
    private final long targetSplitSize;
    private final ImmutableSet.Builder<DeltaLakeScannedDataFile> scannedFilePaths = ImmutableSet.builder();
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Closer closer = Closer.create();

    private volatile TrinoException trinoException;

    public DeltaLakeSplitSource(
            SchemaTableName tableName,
            Stream<DeltaLakeSplit> splits,
            ExecutorService executor,
            int maxSplitsPerSecond,
            int maxOutstandingSplits,
            Duration dynamicFilteringWaitTimeout,
            boolean recordScannedFiles,
            ConnectorSession session,
            Optional<Set<DeltaLakeColumnHandle>> projectedColumns)
    {
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);
        closer.register(queue::finish);
        this.recordScannedFiles = recordScannedFiles;
        this.compositeSplitsEnabled = !recordScannedFiles
                && isCompositeSplitsEnabled(session)
                && projectedColumns
                .map(columns -> columns.stream().allMatch(DeltaLakeSplitSource::isColumnCompatibleWithCompositeSplits))
                .orElse(true);
        this.targetSplitSize = getMaxSplitSize(session).toBytes();
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeout.toMillis();
        closer.register(splits::close);
        queueSplits(splits, queue, executor)
                .exceptionally(throwable -> {
                    // set trinoException before finishing the queue to ensure failure is observed instead of successful completion
                    // (the field is declared as volatile to make sure that the change is visible right away to other threads)
                    trinoException = new TrinoException(findErrorCode(throwable), "Failed to generate splits for " + this.tableName, throwable);
                    try {
                        // Finish the queue to wake up threads from queue.getBatchAsync()
                        queue.finish();
                    }
                    catch (Exception e) {
                        // if we can't finish the queue, consumers that might be waiting for more elements will remain blocked indefinitely
                        LOG.error(e, "Could not communicate split generation error for %s to query; this may cause it to be blocked", tableName);
                    }
                    return null;
                });
    }

    private ErrorCodeSupplier findErrorCode(Throwable throwable)
    {
        return getCausalChain(throwable).stream()
                .filter(TrinoException.class::isInstance)
                .map(TrinoException.class::cast)
                .findFirst()
                .map(t -> (ErrorCodeSupplier) t::getErrorCode)
                .orElse(GENERIC_INTERNAL_ERROR);
    }

    @Override
    public long getRequestedDynamicFilterWaitTimeoutMillis()
    {
        return dynamicFilteringWaitTimeoutMillis;
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        if (trinoException != null) {
            return toCompletableFuture(immediateFailedFuture(trinoException));
        }

        return toCompletableFuture(Futures.transform(
                fetchSplits(maxSize),
                splits -> {
                    TupleDomain<DeltaLakeColumnHandle> dynamicFilterPredicate = dynamicFilterSnapshot.currentPredicate().transformKeys(DeltaLakeColumnHandle.class::cast);
                    if (dynamicFilterPredicate.isNone()) {
                        return ImmutableList.of();
                    }
                    Map<DeltaLakeColumnHandle, Domain> partitionColumnDomains = dynamicFilterPredicate.getDomains().orElseThrow().entrySet().stream()
                            .filter(entry -> entry.getKey().columnType() == DeltaLakeColumnType.PARTITION_KEY)
                            .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
                    List<ConnectorSplit> filteredSplits = splits.stream()
                            .map(DeltaLakeSplit.class::cast)
                            .filter(split -> partitionMatchesPredicate(split.partitionKeys(), partitionColumnDomains) &&
                                    split.statisticsPredicate().overlaps(dynamicFilterPredicate))
                            .collect(toImmutableList());
                    if (recordScannedFiles) {
                        filteredSplits.forEach(split -> {
                            DeltaLakeSplit deltaLakeSplit = (DeltaLakeSplit) split;
                            scannedFilePaths.add(new DeltaLakeScannedDataFile(deltaLakeSplit.path(), deltaLakeSplit.partitionKeys(), deltaLakeSplit.deletionVector()));
                        });
                    }
                    if (compositeSplitsEnabled) {
                        return mergeIntoCompositeSplits(filteredSplits);
                    }
                    return filteredSplits;
                },
                directExecutor()));
    }

    /// With composite splits, waits for a full batch so that packing does not depend on how fast splits are queued.
    private ListenableFuture<List<ConnectorSplit>> fetchSplits(int maxSize)
    {
        if (!compositeSplitsEnabled) {
            return queue.getBatchAsync(maxSize);
        }
        return fetchSplits(maxSize, ImmutableList.of());
    }

    private ListenableFuture<List<ConnectorSplit>> fetchSplits(int maxSize, List<ConnectorSplit> fetched)
    {
        return Futures.transformAsync(
                queue.getBatchAsync(maxSize - fetched.size()),
                splits -> {
                    List<ConnectorSplit> batch = ImmutableList.<ConnectorSplit>builder()
                            .addAll(fetched)
                            .addAll(splits)
                            .build();
                    if (batch.size() >= maxSize || queue.isFinished()) {
                        return immediateFuture(batch);
                    }
                    return fetchSplits(maxSize, batch);
                },
                directExecutor());
    }

    private List<ConnectorSplit> mergeIntoCompositeSplits(List<ConnectorSplit> splits)
    {
        List<DeltaLakeSplit> deltaLakeSplits = splits.stream()
                .map(DeltaLakeSplit.class::cast)
                .collect(toImmutableList());
        ImmutableList.Builder<ConnectorSplit> result = ImmutableList.builder();
        // Composites never span partitions, so the files of a composite share their partition values
        for (Collection<DeltaLakeSplit> partitionSplits : Multimaps.index(deltaLakeSplits, DeltaLakeSplit::partitionKeys).asMap().values()) {
            List<DeltaLakeSplit> currentBatch = new ArrayList<>();
            long currentBatchSize = 0;
            for (DeltaLakeSplit split : partitionSplits) {
                if (!currentBatch.isEmpty() && currentBatchSize + split.length() > targetSplitSize) {
                    result.add(toCompositeOrSingleSplit(currentBatch));
                    currentBatch = new ArrayList<>();
                    currentBatchSize = 0;
                }
                currentBatch.add(split);
                currentBatchSize += split.length();
            }
            result.add(toCompositeOrSingleSplit(currentBatch));
        }
        return result.build();
    }

    private static ConnectorSplit toCompositeOrSingleSplit(List<DeltaLakeSplit> batch)
    {
        if (batch.size() == 1) {
            return batch.getFirst();
        }
        return new CompositeDeltaLakeSplit(batch);
    }

    private static boolean isColumnCompatibleWithCompositeSplits(DeltaLakeColumnHandle column)
    {
        // The GPU path merges the files of a composite into a single fabricated Parquet file, so it cannot
        // produce synthesized columns whose values differ between files ($path, $file_size, $file_modified_time)
        // or rows ($row_position, $row_id). The CPU path handles them, but composites are disabled for both paths
        // because the execution path is chosen at the worker, after splits are generated.
        return column.columnType() == REGULAR || column.columnType() == PARTITION_KEY;
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "Split source must be finished before TableExecuteSplitsInfo is read");
        if (!recordScannedFiles) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.copyOf(scannedFilePaths.build()));
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isFinished()
    {
        if (queue.isFinished()) {
            // Note: queue and trinoException need to be checked in the appropriate order
            // When throwable is set, we want getNextBatch to be called, so that we can propagate the exception
            return trinoException == null;
        }
        return false;
    }

    private static CompletableFuture<Void> queueSplits(Stream<DeltaLakeSplit> splits, AsyncQueue<ConnectorSplit> queue, ExecutorService executor)
    {
        requireNonNull(splits, "splits is null");
        return CompletableFuture.runAsync(
                () -> {
                    splits.map(queue::offer).forEachOrdered(MoreFutures::getFutureValue);
                    queue.finish();
                },
                executor);
    }
}
