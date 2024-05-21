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
package io.trino.plugin.warp.dispatcher.cache;

import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.VaradaColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.services.RowGroupDataService;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.PlanSignature;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CommonStoreIdFinder
{
    private final RowGroupDataService rowGroupDataService;
    private final PlanSignature planSignature;
    private final Map<RowGroupKey, UUID> queryStoreIdCache;

    public CommonStoreIdFinder(RowGroupDataService rowGroupDataService, PlanSignature planSignature)
    {
        this.rowGroupDataService = rowGroupDataService;
        this.planSignature = planSignature;
        this.queryStoreIdCache = new ConcurrentHashMap<>();
    }

    public Optional<UUID> findAndCache(RowGroupKey rowGroupKey)
    {
        Optional<UUID> queryStoreId = Optional.ofNullable(queryStoreIdCache.get(rowGroupKey));
        if (queryStoreId.isEmpty()) {
            RowGroupData rowGroupData = rowGroupDataService.get(rowGroupKey);
            if (rowGroupData != null && !rowGroupData.isEmpty()) {
                queryStoreId = find(rowGroupData, planSignature.getColumns());
                queryStoreId.ifPresent(id -> queryStoreIdCache.put(rowGroupKey, id));
            }
        }
        return queryStoreId;
    }

    public Optional<UUID> getFromCache(RowGroupKey rowGroupKey)
    {
        return Optional.ofNullable(queryStoreIdCache.get(rowGroupKey));
    }

    private Optional<UUID> find(RowGroupData rowGroupData, List<CacheColumnId> requiredColumns)
    {
        if (rowGroupData.getValidWarmUpElements().size() < requiredColumns.size()) {
            return Optional.empty();
        }

        Map<VaradaColumn, Set<UUID>> columnToWeStoreIds = new HashMap<>();
        for (WarmUpElement warmUpElement : rowGroupData.getValidWarmUpElements()) {
            columnToWeStoreIds
                    .computeIfAbsent(warmUpElement.getVaradaColumn(), _ -> new HashSet<>())
                    .add(warmUpElement.getStoreId());
        }

        RegularColumn pickedColumn = new RegularColumn(requiredColumns.getFirst().toString());
        Set<UUID> storeIds = columnToWeStoreIds.getOrDefault(pickedColumn, Collections.emptySet());
        for (int i = 1; i < requiredColumns.size() && !storeIds.isEmpty(); i++) {
            pickedColumn = new RegularColumn(requiredColumns.get(i).toString());
            Set<UUID> columnStoreIds = columnToWeStoreIds.getOrDefault(pickedColumn, Collections.emptySet());
            storeIds.retainAll(columnStoreIds);
        }
        return storeIds.stream().findFirst();
    }
}
