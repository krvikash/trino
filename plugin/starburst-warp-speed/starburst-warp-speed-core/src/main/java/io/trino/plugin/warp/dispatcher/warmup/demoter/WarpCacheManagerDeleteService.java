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
package io.trino.plugin.warp.dispatcher.warmup.demoter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.cache.CacheMgrWarmupRuleService;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.plugin.warp.dispatcher.warmup.WarmupProperties;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.warmup.model.CacheManagerRule;
import org.apache.commons.collections4.CollectionUtils;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.warp.dispatcher.warmup.WarmupProperties.NO_EXPIRY;
import static java.util.Objects.requireNonNull;

@Singleton
public class WarpCacheManagerDeleteService
        implements WarpDeleteService
{
    private static final Logger logger = Logger.get(WarpCacheManagerDeleteService.class);
    private final RowGroupDataService rowGroupDataService;
    private final CacheMgrWarmupRuleService cacheMgrWarmupRuleService;

    private final WarmupProperties defaultWarmupProperties = new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, 10, NO_EXPIRY, TransformFunction.NONE);

    @Inject
    public WarpCacheManagerDeleteService(RowGroupDataService rowGroupDataService, CacheMgrWarmupRuleService cacheMgrWarmupRuleService)
    {
        this.rowGroupDataService = requireNonNull(rowGroupDataService);
        this.cacheMgrWarmupRuleService = requireNonNull(cacheMgrWarmupRuleService);
    }

    @Override
    public TupleRankResult buildTupleRank(List<TupleFilter> tupleFilters, boolean forceDeleteFailedObjects)
    {
        List<RowGroupData> rowGroupDataList = rowGroupDataService.getAll();
        Map<String, CacheManagerRule> warmupRules = cacheMgrWarmupRuleService.getAll();

        Instant now = Instant.now();

        List<TupleRank> tupleRankList = new ArrayList<>();
        List<TupleRank> immediateObjects = new ArrayList<>();
        List<TupleRank> failedObjects = new ArrayList<>();
        for (RowGroupData rowGroupData : rowGroupDataList) {
            if (CollectionUtils.isNotEmpty(tupleFilters)) {
                logger.info("TupleFilter is not supported in cacheManager");
            }
            CacheManagerRule rule = warmupRules.get(rowGroupData.getRowGroupKey().table());
            //Warmup Type is not supported in cacheManager
            WarmupProperties warmupProperties = rule == null ? defaultWarmupProperties :
                    new WarmupProperties(WarmUpType.WARM_UP_TYPE_DATA, rule.priority(), (int) rule.ttl().toSeconds(), TransformFunction.NONE);

            TupleRank tupleRank = new TupleRank(warmupProperties, getWarmupElementWithMaxLastUsedTimestamp(rowGroupData).orElse(null), rowGroupData.getRowGroupKey());
            if (forceDeleteFailedObjects && !rowGroupData.getWarmUpElements().stream().allMatch(WarmUpElement::isValid)) {
                logger.debug("add failed Row to failedObjects: rowGroupKey = %s", rowGroupData.getRowGroupKey());
                failedObjects.add(tupleRank);
            }
            else if (isDeleteImmediatelyObject(tupleRank, now, tupleFilters)) {
                logger.debug("add warmupElement to ImmediateObject: rowGroupKey = %s", rowGroupData.getRowGroupKey());
                immediateObjects.add(tupleRank);
            }
            else {
                tupleRankList.add(tupleRank);
                logger.debug("add warmupElement to tupleRank: rowGroupKey = %s, warmupType = %s, priority = %s", rowGroupData.getRowGroupKey(), warmupProperties.warmUpType().name(), warmupProperties.priority());
            }
        }
        return new TupleRankResult(tupleRankList, immediateObjects, failedObjects);
    }

    private Optional<WarmUpElement> getWarmupElementWithMaxLastUsedTimestamp(RowGroupData rowGroupData)
    {
        if (rowGroupData == null || rowGroupData.getWarmUpElements() == null || rowGroupData.getWarmUpElements().isEmpty()) {
            return Optional.empty();
        }
        return rowGroupData.getWarmUpElements().stream().max(Comparator.comparingLong(WarmUpElement::getLastUsedTimestamp));
    }

    @Override
    public long delete(List<TupleRank> tupleRankList, DemoteContext demoteContext, boolean deleteEmptyRowGroups)
            throws ExecutionException, InterruptedException
    {
        AtomicInteger numDeleted = new AtomicInteger();
        tupleRankList.forEach(tupleRank -> {
            RowGroupData rowGroupData = rowGroupDataService.get(tupleRank.rowGroupKey());
            numDeleted.addAndGet(rowGroupData.getWarmUpElements().size());
            rowGroupDataService.deleteData(rowGroupData, true);
        });
        return numDeleted.get();
    }
}
