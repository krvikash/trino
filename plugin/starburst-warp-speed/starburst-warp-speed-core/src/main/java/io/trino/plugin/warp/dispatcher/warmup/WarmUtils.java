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

import io.airlift.log.Logger;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.cloudvendors.config.CloudVendorConfig;
import io.trino.plugin.warp.cloudvendors.config.StoreType;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.RowGroupData;
import io.trino.plugin.warp.dispatcher.model.RowGroupKey;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.warmup.model.WarmupRule;
import io.trino.spi.connector.ConnectorSession;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class WarmUtils
{
    public static final int FAST_WARMING_VERSION = 8;

    private static final Logger logger = Logger.get(WarmUtils.class);

    private WarmUtils()
    {
        logger.debug("fast warming version %d", FAST_WARMING_VERSION);
    }

    public static boolean isImportExportEnabled(
            GlobalConfig globalConfig,
            CloudVendorConfig cloudVendorConfig,
            ConnectorSession session)
    {
        Boolean sessionEnabled = WarpSessionProperties.getEnableImportExport(session);
        return sessionEnabled != null ? sessionEnabled :
                globalConfig.getEnableImportExport() &&
                        cloudVendorConfig.getStoreType() != StoreType.LOCAL;
    }

    public static String getRowGroupStorageObjectName(RowGroupKey rowGroupKey, String path)
    {
        return rowGroupKey.stringFileNameRepresentation(path + "/" + FAST_WARMING_VERSION + "/");
    }

    public static String getCloudPath(RowGroupKey rowGroupKey, String cloudImportExportPath)
    {
        return getRowGroupStorageObjectName(rowGroupKey, cloudImportExportPath);
    }

    public static Optional<WarmupRule> findMostRelevantRuleForWarmupElement(RowGroupData rowGroupData,
            WarmUpElement warmUpElement,
            List<WarmupRule> rulesForWarmupElement)
    {
        Map<RegularColumn, String> partitionKeys = rowGroupData
                .getPartitionKeys()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> (RegularColumn) entry.getKey(),
                        Map.Entry::getValue));

        return Objects.nonNull(rulesForWarmupElement) ?
                rulesForWarmupElement.stream()
                        .filter(warmupRule -> warmUpElement.getWarmUpType() == warmupRule.getWarmUpType())
                        .filter(warmupRule -> (isEmptyCollection(warmupRule.getPredicates()) ||
                                warmupRule.getPredicates().stream().allMatch(warmupPredicateRule -> warmupPredicateRule.test(partitionKeys))))
                        .max(WorkerWarmingService.warmupRuleComparator)
                : Optional.empty();
    }

    public static boolean isEmptyCollection(Collection<?> collection)
    {
        return collection == null || collection.isEmpty();
    }
}
