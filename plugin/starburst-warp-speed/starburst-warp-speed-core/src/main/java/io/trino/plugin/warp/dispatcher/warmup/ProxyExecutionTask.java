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

import com.google.common.eventbus.EventBus;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherSplit;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.PartitionKey;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.query.classifier.QueryClassifier;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.events.WarmingFinishedEvent;
import io.trino.plugin.warp.dispatcher.warmup.warmers.StorageWarmerService;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmingManager;
import io.trino.plugin.warp.dispatcher.warmup.warmers.WarmupElementsCreator;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.plugin.warp.tools.util.StopWatch;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableCredentials;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.warp.storage.flows.FlowsSequencer.INVALID_FLOW_ID;
import static java.util.Objects.requireNonNull;

public class ProxyExecutionTask
        extends WorkerWarmerBaseTask
{
    private static final Logger logger = Logger.get(ProxyExecutionTask.class);

    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final EventBus eventBus;
    private final GlobalConfig globalConfig;
    private final StorageWarmerService storageWarmerService;
    private final int executionTaskPriority;
    private final ShapingLogger shapingLogger;

    public ProxyExecutionTask(
            WarmExecutionTaskFactory warmExecutionTaskFactory,
            EventBus eventBus,
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            WarmingManager warmingManager,
            WarmingServiceStats warmingServiceStats,
            WorkerWarmingService workerWarmingService,
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
            GlobalConfig globalConfig,
            QueryClassifier queryClassifier,
            WarmupElementsCreator warmupElementsCreator,
            NativeStorageStateHandler nativeStorageStateHandler,
            int iterationCount,
            int executionTaskPriority,
            WorkerTaskExecutorService workerTaskExecutorService,
            StorageWarmerService storageWarmerService,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        super(warmExecutionTaskFactory,
                workerTaskExecutorService,
                warmingServiceStats,
                warmingManager,
                workerWarmingService,
                shapingLoggerFactory,
                connectorPageSourceProvider,
                transactionHandle,
                session,
                dispatcherTableHandle,
                tableCredentials,
                rowGroupKey,
                columns,
                dispatcherSplit,
                dynamicFilter,
                rowGroupDataService,
                queryClassifier,
                warmupElementsCreator,
                nativeStorageStateHandler,
                iterationCount);

        this.globalConfig = requireNonNull(globalConfig);
        shapingLogger = requireNonNull(shapingLoggerFactory).getInstance(this.getClass());

        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.eventBus = requireNonNull(eventBus);
        this.executionTaskPriority = executionTaskPriority;
        this.storageWarmerService = storageWarmerService;
    }

    @Override
    public int getPriority()
    {
        return executionTaskPriority;
    }

    @Override
    public void taskScheduled()
    {
        statsWarmingService.incwarm_scheduled();
    }

    @Override
    protected void warm(WarmData dataToWarm)
    {
        workerWarmingService.warmTaskStarted();
        SchemaTableName schemaTableName = new SchemaTableName(rowGroupKey.schema(), rowGroupKey.table());
        List<WarmUpElement> warmupElements;
        RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
        try {
            warmupElements = warmupElementsCreator.createWarmupElements(
                    rowGroupKey,
                    dataToWarm.requiredWarmUpTypeMap(),
                    schemaTableName,
                    dataToWarm.columnHandleList());
        }
        catch (Exception e) {
            logFailure(e);
            statsWarmingService.incwarm_failed();
            abortWarm(dataToWarm != null && dataToWarm.txMemoryReserved());
            return;
        }

        if (warmupElements.isEmpty()) {
            abortWarm(dataToWarm.txMemoryReserved());
            return;
        }
        Map<WarpColumn, String> partitionKeys = getPartitionKeys(dispatcherSplit);
        if (dataToWarm.warmExecutionState() == WarmExecutionState.EMPTY_ROW_GROUP) {
            boolean warmSuccess = true;
            try {
                statsWarmingService.incwarm_started();
                if (rowGroupData == null) {
                    warmingManager.saveEmptyRowGroup(rowGroupKey, warmupElements, partitionKeys);
                }
                else {
                    warmingManager.warmEmptyRowGroup(rowGroupKey, warmupElements);
                }
            }
            catch (Exception e) {
                logFailure(e);
                statsWarmingService.incwarm_failed();
                warmSuccess = false;
            }
            finally {
                abortWarm(dataToWarm.txMemoryReserved());
                if (warmSuccess) {
                    eventBus.post(new WarmingFinishedEvent(rowGroupKey, session));
                }
                statsWarmingService.incwarm_accomplished();
            }
            return;
        }

        long flowId = INVALID_FLOW_ID;
        StopWatch stopWatch = new StopWatch();
        boolean skipWait = false;
        try {
            skipWait = storageWarmerService.isLoaderAvailable();
            if (!skipWait) {
                storageWarmerService.waitForLoaders();
            }
            flowId = storageWarmerService.tryRunningWarmFlow(rowGroupKey);
            stopWatch.start();
            statsWarmingService.incwarm_started();
            statsWarmingService.addwaiting_for_lock_nano(stopWatch.getNanoTime());
            warmingManager.warm(
                    rowGroupKey,
                    connectorPageSourceProvider,
                    transactionHandle,
                    session,
                    dispatcherTableHandle,
                    tableCredentials,
                    dispatcherSplit,
                    dataToWarm.columnHandleList(),
                    dataToWarm.requiredWarmUpTypeMap(),
                    warmupElements,
                    partitionKeys,
                    skipWait);
        }
        catch (Exception e) {
            logFailure(e);
            if (flowId != INVALID_FLOW_ID) {
                statsWarmingService.incwarm_failed();
            }
        }
        finally {
            boolean releaseTx = dataToWarm.txMemoryReserved();
            storageWarmerService.releaseLoaderThread(skipWait);
            if (storageWarmerService.finishWarm(flowId, releaseTx, true, true)) {
                statsWarmingService.incwarm_accomplished();
            }
            stopWatch.stop();
            statsWarmingService.addexecution_time_nano(stopWatch.getNanoTime());
            logger.debug("warm flow finished nano sec = %d", stopWatch.getNanoTime());
            workerWarmingService.warmTaskFinished();
        }

        // Just a precaution - make sure we're not stuck on an infinite loop of warmups.
        if (iterationCount >= globalConfig.getMaxWarmupIterationsPerQuery()) {
            shapingLogger.error(
                    "Max iteration count has reached (%d), won't try to warm again. rowGroupKey=%s, warpColumns=%s, dataToWarm=%s",
                    globalConfig.getMaxWarmupIterationsPerQuery(),
                    rowGroupKey,
                    columns.stream().map(dispatcherProxiedConnectorTransformer::getWarpRegularColumn).collect(Collectors.toList()),
                    dataToWarm);
            eventBus.post(new WarmingFinishedEvent(rowGroupKey, session));
        }
        else {
            runAnotherWarmUpIteration();
        }
    }

    private void runAnotherWarmUpIteration()
    {
        WarmData dataToWarm = getWarmData(true);
        WarmExecutionState warmExecutionState = dataToWarm.warmExecutionState();
        switch (warmExecutionState) {
            case WARM -> nextTask = Optional.of(createProxyExecutionTask((int) dataToWarm.highestPriority()));
            case EMPTY_ROW_GROUP, NOTHING_TO_WARM -> eventBus.post(new WarmingFinishedEvent(rowGroupKey, session));
            default -> throw new RuntimeException(String.format(Locale.US, "state: %s is not valid, only warm or NOTHING_TO_WARM are valid", warmExecutionState));
        }
    }

    private Map<WarpColumn, String> getPartitionKeys(DispatcherSplit dispatcherSplit)
    {
        return dispatcherSplit.getPartitionKeys().stream().collect(Collectors.toMap(PartitionKey::regularColumn, PartitionKey::partitionValue));
    }

    @Override
    protected WarmData getWarmData()
    {
        return getWarmData(false);
    }

    protected void logFailure(Exception e)
    {
        if (!(e instanceof TrinoException || e instanceof UnsupportedOperationException)) {
            shapingLogger.error(e, "warm failed %s", rowGroupKey);
        }
    }

    private void abortWarm(boolean releaseTx)
    {
        storageWarmerService.finishWarm(INVALID_FLOW_ID, releaseTx, false, false);
        workerWarmingService.warmTaskFinished();
    }
}
