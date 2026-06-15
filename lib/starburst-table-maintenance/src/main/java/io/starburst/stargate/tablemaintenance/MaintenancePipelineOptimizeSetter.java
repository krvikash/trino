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
package io.starburst.stargate.tablemaintenance;

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.FixedCheckpointInterval;
import io.starburst.stargate.tablemaintenance.partitioned.PartitionBasedMaintenanceIteratorFactory;
import io.starburst.stargate.tablemaintenance.partitioned.PartitionedBasedMaintenanceDetails;
import io.trino.spi.connector.CatalogSchemaTableName;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static io.starburst.stargate.tablemaintenance.MaintenanceConstants.ERROR_CODES_FOR_NESTED_PARTITION_OPTIMIZE;
import static io.starburst.stargate.tablemaintenance.MaintenanceConstants.ERROR_CODE_NAMES_FOR_NESTED_PARTITION_OPTIMIZE;
import static io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.createNextIntervalForLatestCheckpoint;
import static io.starburst.stargate.tablemaintenance.OptimizeCheckpointInterval.shouldMakeOptimizeCheckpointsSmaller;
import static java.util.Objects.requireNonNull;

public class MaintenancePipelineOptimizeSetter<HistoryType extends HavingStatus, DebugInfoType extends HavingTableOptimizeStatus>
{
    private static final Logger log = Logger.get(MaintenancePipelineOptimizeSetter.class);
    private static final String COMPLETED = "Completed";

    private final TableMaintenancePipeline.Builder pipelineBuilder;

    private CatalogSchemaTableName tableName;
    private Optional<String> serializedQueueEntry;
    private Optional<HistoryType> previousMaintenanceHistoryEntry;
    private Function<HistoryType, Optional<DebugInfoType>> maintenanceHistoryDebugInfoExtractor;
    private LocalDateTime optimizeEndTime;
    private Function<DebugInfoType, List<? extends MaintenanceErrorSupplier>> debugInfoMaintenanceErrorExtractor;
    private Optional<PartitionBasedMaintenanceIteratorFactory> partitionBasedMaintenanceIteratorFactory;
    private DataSize optimizeFileSizeThreshold;

    MaintenancePipelineOptimizeSetter(TableMaintenancePipeline.Builder pipelineBuilder)
    {
        this.pipelineBuilder = requireNonNull(pipelineBuilder, "pipelineBuilder is null");
    }

    public MaintenancePipelineOptimizeSetter<HistoryType, DebugInfoType> withTableName(CatalogSchemaTableName tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public MaintenancePipelineOptimizeSetter<HistoryType, DebugInfoType> withSerializedQueueEntry(Optional<String> debugLogSerializedQueueEntry)
    {
        this.serializedQueueEntry = debugLogSerializedQueueEntry;
        return this;
    }

    public MaintenancePipelineOptimizeSetter<HistoryType, DebugInfoType> withPreviousMaintenanceHistoryEntry(Optional<HistoryType> previousMaintenanceHistoryEntry)
    {
        this.previousMaintenanceHistoryEntry = previousMaintenanceHistoryEntry;
        return this;
    }

    public MaintenancePipelineOptimizeSetter<HistoryType, DebugInfoType> withMaintenanceHistoryDebugInfoExtractor(Function<HistoryType, Optional<DebugInfoType>> maintenanceHistoryDebugInfoExtractor)
    {
        this.maintenanceHistoryDebugInfoExtractor = maintenanceHistoryDebugInfoExtractor;
        return this;
    }

    public MaintenancePipelineOptimizeSetter<HistoryType, DebugInfoType> withOptimizeEndTime(LocalDateTime optimizeEndTime)
    {
        this.optimizeEndTime = optimizeEndTime;
        return this;
    }

    public MaintenancePipelineOptimizeSetter<HistoryType, DebugInfoType> withDebugInfoMaintenanceErrorExtractor(Function<DebugInfoType, List<? extends MaintenanceErrorSupplier>> debugInfoMaintenanceErrorExtractor)
    {
        this.debugInfoMaintenanceErrorExtractor = debugInfoMaintenanceErrorExtractor;
        return this;
    }

    public MaintenancePipelineOptimizeSetter<HistoryType, DebugInfoType> withPartitionBasedMaintenanceIteratorFactory(Optional<PartitionBasedMaintenanceIteratorFactory> partitionBasedMaintenanceIteratorFactory)
    {
        this.partitionBasedMaintenanceIteratorFactory = partitionBasedMaintenanceIteratorFactory;
        return this;
    }

    public MaintenancePipelineOptimizeSetter<HistoryType, DebugInfoType> withOptimizeFileSizeThreshold(DataSize optimizeFileSizeThreshold)
    {
        this.optimizeFileSizeThreshold = optimizeFileSizeThreshold;
        return this;
    }

    public void setOptimizeDetailsToPipeline()
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(serializedQueueEntry, "serializedQueueEntry is null");
        requireNonNull(previousMaintenanceHistoryEntry, "previousMaintenanceHistoryEntry is null");
        requireNonNull(maintenanceHistoryDebugInfoExtractor, "maintenanceHistoryDebugInfoExtractor is null");
        requireNonNull(optimizeEndTime, "optimizeEndTime is null");
        requireNonNull(debugInfoMaintenanceErrorExtractor, "debugInfoMaintenanceErrorExtractor is null");
        requireNonNull(partitionBasedMaintenanceIteratorFactory, "partitionBasedMaintenanceIteratorFactory is null");
        requireNonNull(optimizeFileSizeThreshold, "optimizeFileSizeThreshold is null");

        int requestedNumberOfPartitionColumns = determineNumberOfPartitionColumns();
        if (requestedNumberOfPartitionColumns > 1) {
            serializedQueueEntry.ifPresent(queueEntry ->
                    log.info("Previous partition-based optimize failed with OOM, using %d partition columns: table: [%s], work: [%s]", requestedNumberOfPartitionColumns, tableName, queueEntry));
        }
        Optional<PartitionedBasedMaintenanceDetails> partitionedTableOptimizeQueriesIterator = partitionBasedMaintenanceIteratorFactory.flatMap(iteratorFactory ->
                iteratorFactory.createPartitionedTableOptimizeQueriesIterator(tableName, requestedNumberOfPartitionColumns));
        partitionedTableOptimizeQueriesIterator.ifPresentOrElse(
                this::setPartitionBasedOptimize,
                this::setFileModifiedTimeCheckpointedOptimize);
    }

    private void setPartitionBasedOptimize(PartitionedBasedMaintenanceDetails partitionBasedOptimize)
    {
        serializedQueueEntry.ifPresent(queueEntry ->
                log.info("Setting partition based optimize: [%s]", queueEntry));
        pipelineBuilder.withOptimizeQueryIterator(partitionBasedOptimize.queryIterator());
        pipelineBuilder.withTableOptimizeStatus(partitionBasedOptimize.optimizeStatus());
    }

    private void setFileModifiedTimeCheckpointedOptimize()
    {
        serializedQueueEntry.ifPresent(queueEntry ->
                log.info("Setting file modified time based optimize: [%s]", queueEntry));
        if (previousMaintenanceHistoryEntry.isEmpty()) {
            serializedQueueEntry.ifPresent(queueEntry ->
                    log.info("No previous maintenance history entry found, using FixedCheckpointInterval optimize queries: [%s]", queueEntry));
            setInitialCheckpointedOptimizeQueriesIterator();
            return;
        }

        HistoryType maintenanceHistoryEntry = previousMaintenanceHistoryEntry.get();
        Optional<DebugInfoType> maintenanceDebugInfoMaybe = maintenanceHistoryDebugInfoExtractor.apply(maintenanceHistoryEntry);
        if (maintenanceDebugInfoMaybe.isEmpty()) {
            serializedQueueEntry.ifPresent(queueEntry ->
                    log.info("No maintenance debug info found for account, using FixedCheckpointInterval optimize queries: [%s]", queueEntry));
            setInitialCheckpointedOptimizeQueriesIterator();
            return;
        }

        DebugInfoType maintenanceDebugInfo = maintenanceDebugInfoMaybe.get();
        if (COMPLETED.equals(maintenanceHistoryEntry.status())) {
            TableOptimizeStatus tableOptimizeStatus = maintenanceDebugInfo.tableOptimizeStatus();
            switch (tableOptimizeStatus) {
                case FileModifiedTimeBasedOptimizeStatus fileModifiedTimeBasedOptimizeStatus -> {
                    serializedQueueEntry.ifPresent(queueEntry ->
                            log.info("Using checkpointed FixedCheckpointInterval optimize queries for queue entry: [%s], checkpoint: [%s]",
                                    queueEntry,
                                    fileModifiedTimeBasedOptimizeStatus.latestOptimizeCheckpoint()));
                    OptimizeQueryIterator checkpointedOptimizeQueryIterator = generateCheckpointedOptimizeQueries(
                            new FixedCheckpointInterval(),
                            fileModifiedTimeBasedOptimizeStatus.latestOptimizeCheckpoint());
                    pipelineBuilder.withOptimizeQueryIterator(checkpointedOptimizeQueryIterator);
                    return;
                }
                case PartitionColumnBasedOptimizeStatus _ -> {
                    setInitialCheckpointedOptimizeQueriesIterator();
                    return;
                }
            }
        }

        createIcebergOptimizeIteratorForPreviouslyFailedMaintenance(maintenanceDebugInfo);
    }

    private void createIcebergOptimizeIteratorForPreviouslyFailedMaintenance(
            DebugInfoType maintenanceDebugInfo)
    {
        List<? extends MaintenanceErrorSupplier> lastOptimizeDebugInfos = debugInfoMaintenanceErrorExtractor.apply(maintenanceDebugInfo);
        if (lastOptimizeDebugInfos.isEmpty()) {
            serializedQueueEntry.ifPresent(queueEntry ->
                    log.info("No previous optimize debug info found for account, using FixedCheckpointInterval optimize queries.: table: [%s], work: [%s]", tableName, queueEntry));
            setInitialCheckpointedOptimizeQueriesIterator();
            return;
        }

        Optional<? extends MaintenanceErrorSupplier> mostRecentlyErroredQueryDebugInfo = lastOptimizeDebugInfos.reversed().stream()
                .filter(operationDebugInfo -> operationDebugInfo.queryId().isPresent())
                .findFirst();
        TableOptimizeStatus tableOptimizeStatus = maintenanceDebugInfo.tableOptimizeStatus();
        switch (tableOptimizeStatus) {
            case FileModifiedTimeBasedOptimizeStatus fileModifiedTimeBasedOptimizeStatus -> {
                if (shouldMakeOptimizeCheckpointsSmaller(mostRecentlyErroredQueryDebugInfo)) {
                    serializedQueueEntry.ifPresent(queueEntry ->
                            log.info("Most recent optimize query debug info indicate need for recreating an interval: table: [%s], work: [%s], latestOptimizeDebugInfo: [%s].",
                                    tableName,
                                    queueEntry,
                                    mostRecentlyErroredQueryDebugInfo));
                    OptimizeCheckpointInterval nextCheckpointInterval = createNextIntervalForLatestCheckpoint(
                            fileModifiedTimeBasedOptimizeStatus.latestUsedInterval(),
                            fileModifiedTimeBasedOptimizeStatus.latestOptimizeCheckpoint());
                    OptimizeQueryIterator iterator = generateCheckpointedOptimizeQueries(
                            nextCheckpointInterval,
                            fileModifiedTimeBasedOptimizeStatus.latestOptimizeCheckpoint());
                    pipelineBuilder.withOptimizeQueryIterator(iterator);
                    return;
                }
                // it must've failed with error not related to optimize itself, reuse previous checkpoint interval
                OptimizeQueryIterator iterator = generateCheckpointedOptimizeQueries(
                        fileModifiedTimeBasedOptimizeStatus.latestUsedInterval(),
                        fileModifiedTimeBasedOptimizeStatus.latestOptimizeCheckpoint());
                pipelineBuilder.withOptimizeQueryIterator(iterator);
            }
            case PartitionColumnBasedOptimizeStatus _ -> setInitialCheckpointedOptimizeQueriesIterator();
        }
    }

    private OptimizeQueryIterator generateCheckpointedOptimizeQueries(
            OptimizeCheckpointInterval checkpointInterval,
            Optional<LocalDateTime> latestOptimizeCheckpoint)
    {
        List<String> optimizeCheckpointPredicates = latestOptimizeCheckpoint
                .map(lastCheckpoint -> checkpointInterval.generateRemainingOptimizeCheckpointPredicates(lastCheckpoint, optimizeEndTime))
                .orElseGet(() -> checkpointInterval.generateInitialOptimizeCheckpointPredicates(optimizeEndTime));

        if (optimizeCheckpointPredicates.isEmpty()) {
            log.warn("Unexpected empty optimize checkpoint dates, using FixedCheckpointInterval optimize queries, for table [%s]. Likely the table has never run successful OPTIMIZE.", tableName);
            optimizeCheckpointPredicates = new FixedCheckpointInterval().generateInitialOptimizeCheckpointPredicates(optimizeEndTime);
        }

        final int finalOptimizePredicatesCount = optimizeCheckpointPredicates.size();
        serializedQueueEntry.ifPresent(queueEntry ->
                log.info("Created checkpointed optimize query iterator for table: [%s], predicates count: [%d]. work: [%s]",
                        tableName,
                        finalOptimizePredicatesCount,
                        queueEntry));

        pipelineBuilder.withTableOptimizeStatus(new FileModifiedTimeBasedOptimizeStatus(
                latestOptimizeCheckpoint,
                checkpointInterval));

        return new OptimizeQueryIterator(
                tableName,
                optimizeCheckpointPredicates,
                optimizeFileSizeThreshold);
    }

    private void setInitialCheckpointedOptimizeQueriesIterator()
    {
        OptimizeQueryIterator checkpointedOptimizeQueryIterator = generateCheckpointedOptimizeQueries(
                new FixedCheckpointInterval(),
                Optional.empty());
        pipelineBuilder.withOptimizeQueryIterator(checkpointedOptimizeQueryIterator);
    }

    private int determineNumberOfPartitionColumns()
    {
        if (previousMaintenanceHistoryEntry.isEmpty()) {
            return 1;
        }
        HistoryType historyEntry = previousMaintenanceHistoryEntry.get();
        if (COMPLETED.equals(historyEntry.status())) {
            return 1;
        }
        Optional<DebugInfoType> debugInfoMaybe = maintenanceHistoryDebugInfoExtractor.apply(historyEntry);
        if (debugInfoMaybe.isEmpty()) {
            return 1;
        }
        DebugInfoType debugInfo = debugInfoMaybe.get();
        if (!(debugInfo.tableOptimizeStatus() instanceof PartitionColumnBasedOptimizeStatus previousStatus)) {
            return 1;
        }
        List<? extends MaintenanceErrorSupplier> errors = debugInfoMaintenanceErrorExtractor.apply(debugInfo);
        Optional<? extends MaintenanceErrorSupplier> mostRecentError = errors.reversed().stream()
                .filter(error -> error.queryId().isPresent())
                .findFirst();
        if (!shouldUseNestedPartitionOptimize(mostRecentError)) {
            return 1;
        }
        return previousStatus.partitionColumns().size() + 1;
    }

    static boolean shouldUseNestedPartitionOptimize(Optional<? extends MaintenanceErrorSupplier> maintenanceErrorSupplier)
    {
        return maintenanceErrorSupplier.map(maintenanceError -> {
            boolean isErrorCodeNameMatch = maintenanceError.errorCodeName().isPresent()
                    && ERROR_CODE_NAMES_FOR_NESTED_PARTITION_OPTIMIZE.contains(maintenanceError.errorCodeName().get());
            boolean isErrorCodeMatch = maintenanceError.errorCode().isPresent()
                    && ERROR_CODES_FOR_NESTED_PARTITION_OPTIMIZE.contains(maintenanceError.errorCode().getAsInt());
            return isErrorCodeNameMatch || isErrorCodeMatch;
        }).orElse(false);
    }
}
