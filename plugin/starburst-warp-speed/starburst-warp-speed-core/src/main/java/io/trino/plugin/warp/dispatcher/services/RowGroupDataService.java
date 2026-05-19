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
package io.trino.plugin.warp.dispatcher.services;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.dal.RowGroupDataDao;
import io.trino.plugin.warp.dispatcher.model.FastWarmingState;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmState;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarmUpElementState;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.gen.stats.WarmingServiceStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.storage.engine.StorageEngine;
import io.trino.plugin.warp.tools.CatalogNameProvider;
import io.trino.plugin.warp.util.StorageUtils;
import io.trino.spi.NodeManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

@Singleton
public class RowGroupDataService
{
    private static final Logger logger = Logger.get(RowGroupDataService.class);

    private final StorageEngine storageEngine;
    private final GlobalConfig globalConfig;
    private final RowGroupDataDao rowGroupDataDao;
    private final WarmingServiceStats warmingServiceStats;
    private final String nodeIdentifier;
    private final CatalogNameProvider catalogNameProvider;

    @Inject
    public RowGroupDataService(
            RowGroupDataDao rowGroupDataDao,
            StorageEngine storageEngine,
            GlobalConfig globalConfig,
            MetricsManager metricsManager,
            NodeManager nodeManager,
            CatalogNameProvider catalogNameProvider)
    {
        this.rowGroupDataDao = requireNonNull(rowGroupDataDao);
        this.storageEngine = requireNonNull(storageEngine);
        this.globalConfig = requireNonNull(globalConfig);
        this.warmingServiceStats = metricsManager.registerMetric(WarmingServiceStats.create());
        this.nodeIdentifier = requireNonNull(nodeManager).getCurrentNode().getNodeIdentifier();
        this.catalogNameProvider = requireNonNull(catalogNameProvider);
    }

    public RowGroupKey createRowGroupKey(
            String schema,
            String table,
            String path,
            long start,
            long length,
            long fileModifiedTime,
            String deletedFilesHash)
    {
        return new RowGroupKey(
                schema,
                table,
                path,
                start,
                length,
                fileModifiedTime,
                deletedFilesHash,
                catalogNameProvider.get());
    }

    public void save(RowGroupData rowGroupData)
    {
        rowGroupDataDao.save(rowGroupData);
    }

    public void flush(RowGroupKey rowGroupKey)
    {
        rowGroupDataDao.flush(rowGroupKey);
    }

    public void updateEmptyRowGroup(
            RowGroupData rowGroupData,
            List<WarmUpElement> newWarmUpElements,
            List<WarmUpElement> warmUpElementsToDelete)
    {
        List<WarmUpElement> updatedWarmupElements = Stream.concat(rowGroupData.getWarmUpElements().stream().filter(we -> !warmUpElementsToDelete.contains(we)),
                        newWarmUpElements.stream()
                                .map(emptyWeElement -> WarmUpElement.builder(emptyWeElement).totalRecords(0).build()))
                .collect(Collectors.toList());
        RowGroupData updatedRowGroupData = RowGroupData.builder(rowGroupData)
                .warmUpElements(updatedWarmupElements)
                .build();
        save(updatedRowGroupData);
        warmingServiceStats.addwarmup_elements_count(newWarmUpElements.size() - warmUpElementsToDelete.size());
    }

    public RowGroupData get(RowGroupKey rowGroupKey)
    {
        return rowGroupDataDao.get(rowGroupKey);
    }

    public RowGroupData getIfPresent(RowGroupKey rowGroupKey)
    {
        return rowGroupDataDao.getIfPresent(rowGroupKey);
    }

    public RowGroupData reload(RowGroupKey rowGroupKey, RowGroupData origRowGroupData)
    {
        rowGroupDataDao.refresh(rowGroupKey);
        RowGroupData newRowGroupData = get(rowGroupKey);

        if (newRowGroupData == null) {
            // refresh does not propagate exceptions
            // RowGroupData file is corrupted, delete it and invalidate cache
            rowGroupDataDao.delete(rowGroupKey, true);
            logger.error("reload failed. delete corrupted file rowGroupKey %s", rowGroupKey);
            return null;
        }
        if (origRowGroupData != null) {
            return rowGroupDataDao.merge(origRowGroupData, newRowGroupData);
        }
        else {
            return newRowGroupData;
        }
    }

    public List<RowGroupData> getAll()
    {
        return new ArrayList<>(rowGroupDataDao.getAll());
    }

    public void deleteFile(RowGroupData rowGroupData)
    {
        if (rowGroupData == null) {
            return;
        }
        RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
        rowGroupDataDao.delete(rowGroupKey, true);
    }

    public void deleteData(RowGroupData rowGroupData, boolean deleteFromCache)
    {
        if (rowGroupData == null) {
            return;
        }
        RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
        long fileHash = StorageUtils.fileHash64(rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath()));
        long fileModTime = rowGroupKey.fileModifiedTime();
        storageEngine.fileIsAboutToBeDeleted(fileHash, fileModTime, rowGroupData.getNextOffset());
        rowGroupDataDao.delete(rowGroupKey, deleteFromCache);
        logger.debug(
                "deleted rowGroupKey %s offset %d next-export-offset %d",
                rowGroupKey,
                rowGroupData.getNextOffset(),
                rowGroupData.getNextExportOffset());
        warmingServiceStats.incdeleted_row_group_count();
    }

    private void invalidateData(RowGroupData rowGroupData)
    {
        rowGroupDataDao.invalidate(rowGroupData.getRowGroupKey());
    }

    public synchronized RowGroupData getOrCreateRowGroupData(
            RowGroupKey rowGroupKey,
            Map<WarpColumn, String> partitionKeys)
    {
        RowGroupData rowGroupData = get(rowGroupKey);

        if (rowGroupData == null) {
            rowGroupData = RowGroupData.builder()
                    .rowGroupKey(rowGroupKey)
                    .nodeIdentifier(nodeIdentifier)
                    .partitionKeys(partitionKeys)
                    .warmUpElements(Collections.emptyList())
                    .build();
            save(rowGroupData);
            warmingServiceStats.incrow_group_count();
        }
        return rowGroupData;
    }

    public RowGroupData getOrCreateTmpRowGroupData(RowGroupKey rowGroupKey)
    {
        RowGroupData rowGroupData = get(rowGroupKey);

        if (rowGroupData == null) {
            rowGroupData = RowGroupData.builder()
                    .rowGroupKey(rowGroupKey)
                    .nodeIdentifier(nodeIdentifier)
                    .partitionKeys(Collections.emptyMap())
                    .warmUpElements(Collections.emptyList())
                    .build();
            save(rowGroupData);
        }
        return rowGroupData;
    }

    public void updateTmpRowGroupData(
            RowGroupData rowGroupData,
            WarmUpElement warmUpElement,
            int nextOffset,
            int totalRecords)
    {
        Collection<WarmUpElement> existingWarmUpElements = rowGroupData.getWarmUpElements();
        WarmUpElement.Builder warmupElementBuilder = WarmUpElement.builder(warmUpElement).totalRecords(totalRecords);
        if (!warmUpElement.isValid()) {
            warmupElementBuilder
                    .warmState(WarmState.COLD);
        }
        warmUpElement = warmupElementBuilder.build();

        Collection<WarmUpElement> updatedWarmUpElements = new ArrayList<>(existingWarmUpElements);

        updatedWarmUpElements.add(warmUpElement);

        RowGroupData.Builder rowGroupDataBuilder = RowGroupData.builder(rowGroupData).warmUpElements(updatedWarmUpElements);

        if (warmUpElement.isValid()) {
            rowGroupDataBuilder.isEmpty(totalRecords == 0);
        }
        RowGroupData updatedRowGroupData = rowGroupDataBuilder
                .nextOffset(nextOffset)
                .fastWarmingState(FastWarmingState.NOT_EXPORTED)
                .build();
        save(updatedRowGroupData);
    }

    public synchronized RowGroupData updateRowGroupData(
            RowGroupData rowGroupData,
            WarmUpElement warmUpElement,
            int nextOffset,
            int totalRecords)
    {
        Collection<WarmUpElement> existingWarmUpElements = rowGroupData.getWarmUpElements();
        WarmUpElement.Builder warmupElementBuilder = WarmUpElement.builder(warmUpElement).totalRecords(totalRecords);
        if (!warmUpElement.isValid()) {
            warmupElementBuilder
                    .state(addTemporaryFailure(warmUpElement.getState(), System.currentTimeMillis()))
                    .warmState(WarmState.COLD);
        }
        warmUpElement = warmupElementBuilder.build();

        Collection<WarmUpElement> updatedWarmUpElements = new ArrayList<>(existingWarmUpElements);
        Optional<WarmUpElement> weToOverride = Optional.empty();
        for (WarmUpElement we : existingWarmUpElements) {
            if ((Objects.equals(we.getStoreId(), warmUpElement.getStoreId()) || (we.getState().state() == WarmUpElementState.State.FAILED_TEMPORARILY)) && // if there's temporary-failed WE, replace it with the new one, even if it has a different storeId because this is how CacheManager warm-up retries work
                    we.getWarpColumn().equals(warmUpElement.getWarpColumn()) &&
                    we.getWarmUpType().equals(warmUpElement.getWarmUpType())) {
                weToOverride = Optional.of(we);
                break;
            }
        }
        if (weToOverride.isPresent()) {
            updatedWarmUpElements.remove(weToOverride.get());
            warmingServiceStats.addwarm_success_retry_warmup_element(warmUpElement.isValid() && !weToOverride.get().isValid() ? 1 : 0);
        }
        updatedWarmUpElements.add(warmUpElement);

        FastWarmingState fastWarmingState = warmUpElement.isValid() ?
                FastWarmingState.NOT_EXPORTED :
                FastWarmingState.EXPORTED; // nothing to export

        if (FastWarmingState.State.FAILED_PERMANENTLY.equals(rowGroupData.getFastWarmingState().state())) {
            fastWarmingState = rowGroupData.getFastWarmingState();
        }

        RowGroupData.Builder rowGroupDataBuilder = RowGroupData.builder(rowGroupData).warmUpElements(updatedWarmUpElements);

        if (warmUpElement.isValid()) {
            rowGroupDataBuilder.isEmpty(totalRecords == 0);
        }
        RowGroupData updatedRowGroupData = rowGroupDataBuilder
                .nextOffset(nextOffset)
                .fastWarmingState(fastWarmingState)
                .build();
        save(updatedRowGroupData);

        if (warmUpElement.isValid()) {
            warmingServiceStats.incwarmup_elements_count();
        }
        else {
            logger.debug("updateRowGroupData failure, row groupKey = %s, warmupElement=%s", rowGroupData.getRowGroupKey().stringFileNameRepresentation(globalConfig.getLocalStorePath()), warmUpElement);
            warmingServiceStats.incwarm_failed();
        }

        if (warmUpElement.isValid() && totalRecords == 0) {
            warmingServiceStats.incempty_row_group();
        }
        return updatedRowGroupData;
    }

    public void markAsFailed(
            RowGroupKey rowGroupKey,
            Collection<WarmUpElement> proxiedWarmUpElements,
            Map<WarpColumn, String> partitionKeys)
    {
        RowGroupData rowGroupData = get(rowGroupKey);
        if (rowGroupData == null) {
            rowGroupData = RowGroupData.builder()
                    .rowGroupKey(rowGroupKey)
                    .nodeIdentifier(nodeIdentifier)
                    .nextOffset(0)
                    .warmUpElements(Collections.emptyList())
                    .partitionKeys(partitionKeys)
                    .build();
            warmingServiceStats.incrow_group_count();
        }
        for (WarmUpElement failedElement : proxiedWarmUpElements) {
            if (failedElement.isValid()) {
                // should not happen. added in order to protect from unfamiliar error flow
                failedElement = WarmUpElement.builder(failedElement).state(new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY)).build();
            }
            updateRowGroupData(rowGroupData, failedElement, rowGroupData.getNextOffset(), -1);
        }
        flush(rowGroupKey);
    }

    public RowGroupData markAsFailedPermanently(RowGroupData rowGroupData, WarmUpElement warmUpElement)
    {
        WarmUpElement failedElement = WarmUpElement.builder(warmUpElement).state(WarmUpElementState.FAILED_PERMANENTLY).build();
        return updateRowGroupData(rowGroupData, failedElement, rowGroupData.getNextOffset(), -1);
    }

    private WarmUpElementState addTemporaryFailure(WarmUpElementState warmUpElementState, long lastTemporaryFailure)
    {
        if (WarmUpElementState.State.FAILED_PERMANENTLY.equals(warmUpElementState.state()) ||
                warmUpElementState.temporaryFailureCount() >= globalConfig.getMaxWarmRetries()) {
            return WarmUpElementState.FAILED_PERMANENTLY;
        }

        int failureCount = warmUpElementState.temporaryFailureCount() + 1;
        return new WarmUpElementState(WarmUpElementState.State.FAILED_TEMPORARILY, failureCount, lastTemporaryFailure);
    }

    // for debug command
    public void deleteAll()
    {
        rowGroupDataDao.getAll().forEach(rowGroupData -> deleteData(rowGroupData, true));
    }

    // for debug command
    public void invalidateAll()
    {
        rowGroupDataDao.getAll().forEach(this::invalidateData);
    }

    public synchronized void removeElements(RowGroupData rowGroupData, Collection<WarmUpElement> deletedWarmUpElements)
    {
        if (deletedWarmUpElements.isEmpty()) {
            return;
        }

        Collection<WarmUpElement> existingWarmUpElements = rowGroupData.getWarmUpElements();

        if (existingWarmUpElements.size() == deletedWarmUpElements.size()) {
            removeElements(rowGroupData);
            return;
        }

        List<WarmUpElement> updatedWarmUpElements = new ArrayList<>();
        Collection<WarmUpElement> toDeleteWarmUpElements = new ArrayList<>();
        RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
        String fileName = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());

        for (WarmUpElement existingWarmUpElement : existingWarmUpElements) {
            if (deletedWarmUpElements.stream().noneMatch(we -> we.isRepresentTheSameElement(existingWarmUpElement))) {
                updatedWarmUpElements.add(existingWarmUpElement);
            }
            else {
                // keep offsets and update native only for valid warmUpElements
                if (existingWarmUpElement.isValid()) {
                    // no need to keep offsets if file wasn't ever exported
                    if (rowGroupData.getNextExportOffset() != 0) {
                        WarmUpElement warmWarmUpElement = WarmUpElement.builder(existingWarmUpElement)
                                .warmState(WarmState.WARM)
                                .build();
                        updatedWarmUpElements.add(warmWarmUpElement);
                    }
                    toDeleteWarmUpElements.add(existingWarmUpElement);
                }
            }
        }

        RowGroupData newRowGroupData = RowGroupData.builder(rowGroupData)
                .warmUpElements(updatedWarmUpElements)
                .sparseFile(!toDeleteWarmUpElements.isEmpty())
                .build();
        save(newRowGroupData);
        flush(newRowGroupData.getRowGroupKey());

        toDeleteWarmUpElements.forEach(toDeleteWarmUpElement -> storageEngine.filePunchHole(fileName, toDeleteWarmUpElement.getStartOffset(), toDeleteWarmUpElement.getEndOffset()));

        warmingServiceStats.adddeleted_warmup_elements_count(deletedWarmUpElements.size());
    }

    public synchronized void removeElements(RowGroupData rowGroupData)
    {
        // no need to keep offsets if file wasn't ever exported
        if (rowGroupData.getNextExportOffset() == 0) {
            deleteData(rowGroupData, true);
            return;
        }

        Collection<WarmUpElement> existingWarmUpElements = rowGroupData.getWarmUpElements();
        List<WarmUpElement> updatedWarmUpElements = new ArrayList<>();
        RowGroupKey rowGroupKey = rowGroupData.getRowGroupKey();
        String fileName = rowGroupKey.stringFileNameRepresentation(globalConfig.getLocalStorePath());

        for (WarmUpElement existingWarmUpElement : existingWarmUpElements) {
            // keep offsets only for valid warmUpElements
            if (existingWarmUpElement.isValid()) {
                WarmUpElement warmWarmUpElement = existingWarmUpElement;

                if (existingWarmUpElement.isHot()) {
                    warmWarmUpElement = WarmUpElement.builder(existingWarmUpElement)
                            .warmState(WarmState.WARM)
                            .build();
                }
                updatedWarmUpElements.add(warmWarmUpElement);
            }
        }

        // no offsets to keep
        if (updatedWarmUpElements.isEmpty()) {
            deleteData(rowGroupData, true);
            return;
        }

        RowGroupData newRowGroupData = RowGroupData.builder(rowGroupData)
                .warmUpElements(updatedWarmUpElements)
                .sparseFile(true)
                .build();
        save(newRowGroupData);
        flush(newRowGroupData.getRowGroupKey());

        storageEngine.filePunchHole(fileName, 0, rowGroupData.getNextOffset());

        warmingServiceStats.adddeleted_warmup_elements_count(updatedWarmUpElements.size());
    }
}
