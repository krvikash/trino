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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.warp.annotation.Audit;
import io.trino.plugin.warp.config.WarmupDemoterConfig;
import io.trino.plugin.warp.dispatcher.warmup.demoter.TupleFilter;
import io.trino.plugin.warp.dispatcher.warmup.demoter.TupleRankResult;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarmupDemoterService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.WarpDeleteService;
import io.trino.plugin.warp.dispatcher.warmup.demoter.events.WarmupDemoterFinishEvent;
import io.trino.plugin.warp.dispatcher.warmup.events.WarmupDemoterConfigChangedEvent;
import io.trino.plugin.warp.execution.debugtools.ColumnFilter;
import io.trino.plugin.warp.execution.debugtools.FileFilter;
import io.trino.plugin.warp.extension.execution.TaskResource;
import io.trino.plugin.warp.extension.execution.TaskResourceMarker;
import io.trino.plugin.warp.gen.stats.WarmupDemoterStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.capacity.WorkerCapacityManager;
import io.trino.plugin.warp.storage.engine.nativeimpl.NativeStorageStateHandler;
import io.trino.spi.catalog.CatalogName;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

import static io.trino.plugin.warp.dispatcher.warmup.WarmUtils.isEmptyCollection;
import static io.trino.plugin.warp.extension.execution.debugtools.WarmupDemoterTask.WARMUP_DEMOTER_PATH;
import static java.util.Objects.requireNonNull;

// connector is true by default
@TaskResourceMarker(coordinator = false)
@Path(WARMUP_DEMOTER_PATH)
//@Api(value = "Demoter", tags = "Demoter")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class WorkerWarmupDemoterTask
        implements TaskResource
{
    public static final String WARMUP_DEMOTER_START_TASK_NAME = "worker-warmup-demoter-start";
    public static final String WARMUP_DEMOTER_STATUS_TASK_NAME = "worker-warmup-demoter-status";
    public static final String WARMUP_DEMOTER_TUPLE_RANKS_TASK_NAME = "worker-demoter-tuple-ranks";
    public static final String HIGHEST_PRIORITY_KEY = String.format(Locale.US, "%s:highestPriority", WarmupDemoterStats.createKey());
    public static final String MAX_USAGE_THRESHOLD_KEY = String.format(Locale.US, "%s:maxUsageThresholdInPercentage", WarmupDemoterStats.createKey());
    public static final String CLEANUP_USAGE_THRESHOLD_KEY = String.format(Locale.US, "%s:cleanupUsageThresholdInPercentage", WarmupDemoterStats.createKey());
    public static final String TOTAL_USAGE_THRESHOLD_KEY = String.format(Locale.US, "%s:totalUsage", WarmupDemoterStats.createKey());
    public static final String CURRENT_USAGE_THRESHOLD_KEY = String.format(Locale.US, "%s:currentUsage", WarmupDemoterStats.createKey());
    public static final String BATCH_SIZE_KEY = String.format(Locale.US, "%s:batchSize", WarmupDemoterStats.createKey());
    public static final String DEMOTE_SEQUENCE_KEY = String.format(Locale.US, "%s:demoteSequence", WarmupDemoterStats.createKey());
    public static final String EPSILON_KEY = String.format(Locale.US, "%s:epsilon", WarmupDemoterStats.createKey());
    public static final String MAX_ELEMENTS_TO_DEMOTE_ITERATION_KEY = String.format(Locale.US, "%s:maxElementsDemoteInIteration", WarmupDemoterStats.createKey());
    public static final String START_EXECUTION_KEY = String.format(Locale.US, "%s:startExecution", WarmupDemoterStats.createKey());
    public static final String END_EXECUTION_KEY = String.format(Locale.US, "%s:endExecution", WarmupDemoterStats.createKey());
    private static final Logger logger = Logger.get(WorkerWarmupDemoterTask.class);

    private final WarmupDemoterService warmupDemoterService;
    private final WarpDeleteService warpDeleteService;
    private final WarmupDemoterConfig warmupDemoterConfig;
    private final WorkerCapacityManager workerCapacityManager;
    private final CatalogName catalogName;
    private final WarmupDemoterStats globalStatsDemoter;
    private final NativeStorageStateHandler nativeStorageStateHandler;
    private final EventBus eventBus;

    private final AtomicReference<CompletableFuture<WarmupDemoterFinishEvent>> demoteFuture = new AtomicReference<>();

    @Inject
    public WorkerWarmupDemoterTask(
            WarmupDemoterService warmupDemoterService,
            WarpDeleteService warpDeleteService,
            WarmupDemoterConfig warmupDemoterConfig,
            WorkerCapacityManager workerCapacityManager,
            CatalogName catalogName,
            MetricsManager metricsManager,
            EventBus eventBus,
            NativeStorageStateHandler nativeStorageStateHandler)
    {
        this.warmupDemoterService = requireNonNull(warmupDemoterService);
        this.warpDeleteService = requireNonNull(warpDeleteService);
        this.warmupDemoterConfig = warmupDemoterConfig;
        this.workerCapacityManager = workerCapacityManager;
        this.catalogName = catalogName;
        this.nativeStorageStateHandler = requireNonNull(nativeStorageStateHandler);
        this.eventBus = requireNonNull(eventBus);

        this.globalStatsDemoter = metricsManager.registerMetric(WarmupDemoterStats.create());
        eventBus.register(this);
    }

    @POST
    @Audit
    @Path(WARMUP_DEMOTER_START_TASK_NAME)
    // @ApiOperation(value = "start", nickname = "startDemoter", extensions = {@Extension(properties = @ExtensionProperty(name = "exposing-level", value = "DEBUG"))})
    public Map<String, Object> start(WarmupDemoterData warmupDemoterData)
    {
        logger.debug("%s: start warmup demote task", catalogName);

        if (!nativeStorageStateHandler.isStorageAvailable()) {
            logger.warn("storage not initiated");
            return getSkippedResult();
        }

        if (demoteFuture.get() != null) {
            logger.info(
                    "catalog[%s]: SKIPPING start warmup demote task since another is running",
                    catalogName);
            return Map.of();
        }

        logger.debug("catalog[%s]: start warmup demote task", catalogName);
        modifyConfigIfRequired(warmupDemoterData);
        if (!warmupDemoterData.isExecuteDemoter()) {
            return getConfigResults();
        }
        Instant start = Instant.now();
        Map<String, Object> result = new HashMap<>(getConfigResults());
        try {
            logger.debug("catalog[%s]: before calling warmupDemoterService.tryDemoteStart", catalogName);

            demoteFuture.set(new CompletableFuture<>());
            warmupDemoterConfig.setDeleteEmptyRowGroups(true);

            if (!warmupDemoterService.tryDemoteStart()) {
                warmupDemoterConfig.setDeleteEmptyRowGroups(false);
                demoteFuture.get().complete(new WarmupDemoterFinishEvent(false, Map.of()));
            }

            WarmupDemoterFinishEvent finishEvent = demoteFuture.get().get();

            logger.debug("catalog[%s]: finishEvent %s", catalogName, finishEvent);
            if (finishEvent.success()) {
                result.putAll(finishEvent.runResults());
            }
            else {
                result.putAll(getSkippedResult());
            }

            workerCapacityManager.updateCurrentUsage();
        }
        catch (Exception e) {
            result.putAll(getSkippedResult());
            logger.error(e);
        }
        finally {
            CompletableFuture<WarmupDemoterFinishEvent> removed = demoteFuture.getAndSet(null);
            if (removed != null && !removed.isDone()) {
                removed.cancel(true);
            }
        }

        result.put(START_EXECUTION_KEY, LocalTime.ofInstant(start, ZoneId.systemDefault()));
        result.put(END_EXECUTION_KEY, LocalTime.now(ZoneId.systemDefault()));
        overrideUsageValuesFromGlobalDemote(result);

        logger.debug("start::result=%s", result);

        return result;
    }

    @GET
    @Path(WARMUP_DEMOTER_STATUS_TASK_NAME)
    public DemoterStatus status()
    {
        return new DemoterStatus(warmupDemoterService.isExecuting(), warmupDemoterService.getLastExecutionTime());
    }

    @GET
    @Path(WARMUP_DEMOTER_TUPLE_RANKS_TASK_NAME)
    public TupleRankResult getTupleRanks()
    {
        return warpDeleteService.buildTupleRank(List.of(), true);
    }

    public Map<String, Object> getSkippedResult()
    {
        logger.debug("return skipped results");
        return new HashMap<>(globalStatsDemoter.statsCounterMapper());
    }

    public Map<String, Object> getConfigResults()
    {
        Map<String, Object> result = new HashMap<>();
        result.put(HIGHEST_PRIORITY_KEY, warmupDemoterService.getDemoterHighestPriority());
        result.put(MAX_USAGE_THRESHOLD_KEY, warmupDemoterConfig.getMaxUsageThresholdPercentage());
        result.put(CLEANUP_USAGE_THRESHOLD_KEY, warmupDemoterConfig.getCleanupUsageThresholdPercentage());
        result.put(BATCH_SIZE_KEY, warmupDemoterConfig.getBatchSize());
        result.put(EPSILON_KEY, warmupDemoterConfig.getEpsilon());
        result.put(MAX_ELEMENTS_TO_DEMOTE_ITERATION_KEY, warmupDemoterConfig.getMaxElementsToDemoteInIteration());
        return result;
    }

    private void overrideUsageValuesFromGlobalDemote(Map<String, Object> result)
    {
        result.put(CURRENT_USAGE_THRESHOLD_KEY, globalStatsDemoter.getcurrentUsage());
        result.put(TOTAL_USAGE_THRESHOLD_KEY, globalStatsDemoter.gettotalUsage());
    }

    private Thresholds getThresholds(WarmupDemoterData warmupDemoterData)
    {
        if (warmupDemoterData.getWarmupDemoterThreshold() == null) {
            return new Thresholds(
                    warmupDemoterData.getMaxUsageThresholdInPercentage(),
                    warmupDemoterData.getCleanupUsageThresholdInPercentage());
        }

        long currentUsage = workerCapacityManager.getCurrentUsage();
        long totalCapacity = workerCapacityManager.getTotalCapacity();
        WarmupDemoterThreshold threshold = warmupDemoterData.getWarmupDemoterThreshold();
        return new Thresholds(
                calculateThreshold(threshold.maxPercentageFactorThreshold(), currentUsage, totalCapacity),
                calculateThreshold(threshold.cleanupPercentageFactorThreshold(), currentUsage, totalCapacity));
    }

    private double calculateThreshold(double factorThreshold, long currentUsage, long totalCapacity)
    {
        return (currentUsage * factorThreshold) * 100 / totalCapacity;
    }

    private void modifyConfigIfRequired(WarmupDemoterData warmupDemoterData)
    {
        if (warmupDemoterData.isModifyConfig()) {
            if (warmupDemoterData.getBatchSize() > -1) {
                warmupDemoterConfig.setBatchSize(warmupDemoterData.getBatchSize());
                warmupDemoterConfig.setMaxElementsToDemoteInIteration(warmupDemoterData.getBatchSize());
            }
            Thresholds thresholds = getThresholds(warmupDemoterData);
            if (thresholds.maxUsageThreshold() > -1) {
                warmupDemoterConfig.setMaxUsageThresholdPercentage(thresholds.maxUsageThreshold());
            }
            if (thresholds.cleanupUsageThreshold() > -1) {
                warmupDemoterConfig.setCleanupUsageThresholdPercentage(thresholds.cleanupUsageThreshold());
            }
            if (warmupDemoterData.getEpsilon() > -1) {
                warmupDemoterConfig.setEpsilon(warmupDemoterData.getEpsilon());
            }
            if (warmupDemoterData.getMaxElementsToDemoteInIteration() > -1) {
                warmupDemoterConfig.setMaxElementsToDemoteInIteration(warmupDemoterData.getMaxElementsToDemoteInIteration());
            }

            if (warmupDemoterData.getDefaultRuleTtlInSeconds() > 0) {
                warmupDemoterConfig.setDefaultRuleTtlInSeconds(warmupDemoterData.getDefaultRuleTtlInSeconds());
            }

            warmupDemoterService.setTupleFilters(calculateTupleFilter(warmupDemoterData));
            warmupDemoterConfig.setForceDeleteDeadObjects(warmupDemoterData.isForceExecuteDeadObjects());
            warmupDemoterConfig.setForceDeleteFailedObjects(warmupDemoterData.isForceDeleteFailedObjects());
            warmupDemoterConfig.setResetHighestPriority(warmupDemoterData.isResetHighestPriority());
            warmupDemoterConfig.setEnableDemote(warmupDemoterData.isEnableDemoteFeature());

            eventBus.post(new WarmupDemoterConfigChangedEvent());

            logger.info("catalog[%s]: config changed -> %s", catalogName, warmupDemoterConfig);
        }
    }

    private List<TupleFilter> calculateTupleFilter(WarmupDemoterData warmupDemoterData)
    {
        List<TupleFilter> tupleFilters = new ArrayList<>();
        if (warmupDemoterData.getSchemaTableName() != null || !isEmptyCollection(warmupDemoterData.getWarmupElementsData())) {
            tupleFilters.add(new ColumnFilter(warmupDemoterData.getSchemaTableName(), warmupDemoterData.getWarmupElementsData()));
        }
        if (!isEmptyCollection(warmupDemoterData.getFilePaths())) {
            tupleFilters.add(new FileFilter(warmupDemoterData.getFilePaths()));
        }
        return tupleFilters;
    }

    @SuppressWarnings("unused")
    @Subscribe
    private void demoteFinished(WarmupDemoterFinishEvent demoterFinishEvent)
    {
        if (demoteFuture.get() != null) {
            demoteFuture.get().complete(demoterFinishEvent);
        }
    }

    private record Thresholds(double maxUsageThreshold, double cleanupUsageThreshold) {}
}
