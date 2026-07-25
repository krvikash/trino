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
package io.trino.plugin.warp.dispatcher.warmup;

import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.classifier.ClassificationType;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.util.Objects.requireNonNull;

public abstract class WorkerWarmerBaseTask
        implements WorkerSubmittableTask
{
    private final ShapingLogger shapingLogger;

    protected final WarmExecutionTaskFactory warmExecutionTaskFactory;
    protected final WarmingManager warmingManager;
    protected final ConnectorPageSourceProvider connectorPageSourceProvider;
    protected final ConnectorTransactionHandle transactionHandle;
    protected final ConnectorSession session;
    protected final DispatcherTableHandle dispatcherTableHandle;
    protected final Optional<ConnectorTableCredentials> tableCredentials;
    protected final RowGroupKey rowGroupKey;
    protected final List<ColumnHandle> columns;
    protected final DispatcherSplit dispatcherSplit;
    protected final WorkerWarmingService workerWarmingService;
    protected final WarmingServiceStats statsWarmingService;
    protected final DynamicFilter dynamicFilter;
    protected final RowGroupDataService rowGroupDataService;
    protected final QueryClassifier queryClassifier;
    protected final WorkerTaskExecutorService workerTaskExecutorService;
    protected final WarmupElementsCreator warmupElementsCreator;
    protected final NativeStorageStateHandler nativeStorageStateHandler;
    protected final int iterationCount;
    protected final UUID id;
    protected Optional<WorkerSubmittableTask> nextTask = Optional.empty();

    public WorkerWarmerBaseTask(
            WarmExecutionTaskFactory warmExecutionTaskFactory,
            WorkerTaskExecutorService workerTaskExecutorService,
            WarmingServiceStats statsWarmingService,
            WarmingManager warmingManager,
            WorkerWarmingService workerWarmingService,
            ShapingLoggerFactory shapingLoggerFactory,
            ConnectorPageSourceProvider connectorPageSourceProvider,
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            DispatcherTableHandle dispatcherTableHandle,
            Optional<ConnectorTableCredentials> tableCredentials,
            RowGroupKey rowGroupKey,
            List<ColumnHandle> columns,
            DispatcherSplit dispatcherSplit,
            DynamicFilter dynamicFilter,
            RowGroupDataService rowGroupDataService,
            QueryClassifier queryClassifier,
            WarmupElementsCreator warmupElementsCreator,
            NativeStorageStateHandler nativeStorageStateHandler,
            int iterationCount)
    {
        this.warmExecutionTaskFactory = warmExecutionTaskFactory;
        this.statsWarmingService = requireNonNull(statsWarmingService);
        this.connectorPageSourceProvider = requireNonNull(connectorPageSourceProvider);
        this.transactionHandle = requireNonNull(transactionHandle);
        this.session = requireNonNull(session);
        this.dispatcherTableHandle = requireNonNull(dispatcherTableHandle);
        this.tableCredentials = requireNonNull(tableCredentials);
        this.rowGroupKey = requireNonNull(rowGroupKey);
        this.columns = requireNonNull(columns);
        this.dispatcherSplit = requireNonNull(dispatcherSplit);
        this.workerWarmingService = requireNonNull(workerWarmingService);
        this.dynamicFilter = requireNonNull(dynamicFilter);
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.queryClassifier = requireNonNull(queryClassifier);
        this.workerTaskExecutorService = requireNonNull(workerTaskExecutorService);
        this.warmingManager = requireNonNull(warmingManager);
        this.warmupElementsCreator = requireNonNull(warmupElementsCreator);
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
        this.iterationCount = iterationCount;
        this.id = UUID.randomUUID();

        this.shapingLogger = shapingLoggerFactory.getInstance(WorkerWarmerBaseTask.class);
    }

    @Override
    public UUID getId()
    {
        return id;
    }

    @Override
    public RowGroupKey getRowGroupKey()
    {
        return rowGroupKey;
    }

    @Override
    public void run()
    {
        try {
            if (nativeStorageStateHandler.isStorageAvailable()) {
                WarmData dataToWarm = getWarmData();
                WarmExecutionState warmExecutionState = dataToWarm.warmExecutionState();
                if (!warmExecutionState.equals(WarmExecutionState.NOTHING_TO_WARM)) {
                    warm(dataToWarm);
                }
            }
        }
        catch (Exception e) {
            shapingLogger.error(e, "Warming task has failed");
        }
        finally {
            finish();
        }
    }

    protected void finish()
    {
        workerWarmingService.removeRowGroupFromSubmittedRowGroup(rowGroupKey);
        statsWarmingService.incwarm_finished();
        addNextTask();
    }

    protected void addNextTask()
    {
        nextTask.ifPresent(task -> workerTaskExecutorService.submitTask(task, true));
    }

    protected abstract void warm(WarmData dataToWarm);

    protected abstract WarmData getWarmData();

    protected WarmData getWarmData(boolean isDryRun)
    {
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);

        QueryContext baseQueryContext = queryClassifier.getBasicQueryContext(columns, dispatcherTableHandle, dynamicFilter, session, ClassificationType.WARMING);

        QueryContext queryContext;
        if (rowGroupData != null) {
            queryContext = queryClassifier.classify(baseQueryContext, rowGroupData, dispatcherTableHandle, session, ClassificationType.WARMING);
        }
        else {
            queryContext = baseQueryContext;
        }

        return workerWarmingService.getWarmData(
                columns,
                rowGroupKey,
                dispatcherSplit,
                session,
                queryContext,
                isDryRun);
    }

    protected WorkerSubmittableTask createProxyExecutionTask(int priority)
    {
        return warmExecutionTaskFactory.createExecutionTask(
                connectorPageSourceProvider,
                transactionHandle,
                session,
                dispatcherSplit,
                dispatcherTableHandle,
                tableCredentials,
                columns,
                dynamicFilter,
                rowGroupKey,
                workerWarmingService,
                iterationCount,
                priority,
                WorkerTaskExecutorService.TaskExecutionType.PROXY);
    }
}
