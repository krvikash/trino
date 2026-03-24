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
package io.trino.plugin.warp.dispatcher;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.warp.annotation.ForWarp;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.spi.cache.CacheColumnId;
import io.trino.spi.cache.CacheTableId;
import io.trino.spi.cache.ConnectorCacheMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.TupleDomain;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DispatcherCacheMetadata
        implements ConnectorCacheMetadata
{
    private final ConnectorCacheMetadata proxiedConnectorCacheMetadata;
    private final DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider;
    public final int predicateThreshold;
    private final ObjectMapper objectMapper;
    private final ShapingLogger shapingLogger;

    @Inject
    public DispatcherCacheMetadata(
            @ForWarp ConnectorCacheMetadata proxiedConnectorCacheMetadata,
            DispatcherTableHandleBuilderProvider dispatcherTableHandleBuilderProvider,
            GlobalConfig globalConfig,
            ObjectMapperProvider objectMapperProvider,
            ShapingLoggerFactory shapingLoggerFactory)
    {
        this.proxiedConnectorCacheMetadata = requireNonNull(proxiedConnectorCacheMetadata);
        this.dispatcherTableHandleBuilderProvider = requireNonNull(dispatcherTableHandleBuilderProvider);
        predicateThreshold = requireNonNull(globalConfig).getPredicateSimplifyThreshold();
        objectMapper = requireNonNull(objectMapperProvider).get();
        shapingLogger = requireNonNull(shapingLoggerFactory).getInstance(DispatcherCacheMetadata.class);
    }

    @Override
    public Optional<CacheTableId> getCacheTableId(ConnectorTableHandle tableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = (DispatcherTableHandle) tableHandle;
        Optional<CacheTableId> result = proxiedConnectorCacheMetadata.getCacheTableId(dispatcherTableHandle.getProxyConnectorTableHandle());
        if (result.isEmpty()) {
            return result;
        }
        try {
            ImmutableMap.Builder<String, Object> values = ImmutableMap.builder();
            if (dispatcherTableHandle.getWarpExpression().isPresent()) {
                values.put("warpExpression ", dispatcherTableHandle.getWarpExpression().get());
            }
            values.put("cacheId", result.get().toString());
            String cacheTableId = objectMapper.writeValueAsString(values.buildOrThrow());
            result = Optional.of(new CacheTableId(cacheTableId));
        }
        catch (JsonProcessingException e) {
            shapingLogger.error(e, "SUBQUERY CACHE: failed to serialize tableHandle=%s", tableHandle);
            result = Optional.empty();
        }
        return result;
    }

    @Override
    public Optional<CacheColumnId> getCacheColumnId(ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return proxiedConnectorCacheMetadata.getCacheColumnId(((DispatcherTableHandle) tableHandle).getProxyConnectorTableHandle(), columnHandle);
    }

    @Override
    public ConnectorTableHandle getCanonicalTableHandle(ConnectorTableHandle tableHandle)
    {
        DispatcherTableHandle dispatcherTableHandle = ((DispatcherTableHandle) tableHandle);
        ConnectorTableHandle newProxiedConnectorTableHandle = proxiedConnectorCacheMetadata
                .getCanonicalTableHandle(dispatcherTableHandle.getProxyConnectorTableHandle());
        return dispatcherTableHandleBuilderProvider
                .builder(dispatcherTableHandle, predicateThreshold)
                .warpExpression(Optional.empty())
                .fullPredicate(TupleDomain.all())
                .proxiedConnectorTableHandle(newProxiedConnectorTableHandle)
                .build();
    }
}
