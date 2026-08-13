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
package io.trino.plugin.iceberg.catalog.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.spi.Node;
import jakarta.annotation.PreDestroy;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.TrinoRESTObjectMapper;
import org.weakref.jmx.MBeanExport;
import org.weakref.jmx.MBeanExporter;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RestClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(RestResponseCacheExporter.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    public Optional<RestResponseCache> createRestResponseCache(IcebergRestCatalogConfig restConfig, SecurityProperties securityProperties, Node currentNode)
    {
        boolean cacheEnabled = !restConfig.getMetastoreCacheTtl().isZero()
                && !securityProperties.variesByUser()
                // If the cache were used on workers, there would be no way to invalidate it.
                && currentNode.isCoordinator()
                // With per-user sessions, different users may receive different responses
                // (e.g. different credentials) for the same table, so caching is disabled
                // to avoid serving one session's cached response to another.
                && restConfig.getSessionType() == IcebergRestCatalogConfig.SessionType.NONE;
        if (cacheEnabled) {
            return Optional.of(new RestResponseCache(restConfig.getMetastoreCacheTtl(), restConfig.getMetastoreCacheMaximumSize(), restConfig.getMetastoreCacheMaximumEntrySize()));
        }
        return Optional.empty();
    }

    @Provides
    @Singleton
    public RestClientFactory createRestClientFactory(Optional<RestResponseCache> cache)
    {
        RestClientStats stats = new RestClientStats();
        if (cache.isPresent()) {
            ObjectMapper objectMapper = TrinoRESTObjectMapper.mapperWithDeserializer(RawResponse.class, new RawResponseDeserializer());
            return new CachingRestClientFactory(objectMapper, stats, cache.get());
        }
        return new NonCachingRestClientFactory(stats);
    }

    private static class CachingRestClientFactory
            implements RestClientFactory
    {
        private final ObjectMapper objectMapper;
        private final RestClientStats stats;
        private final RestResponseCache cache;

        CachingRestClientFactory(ObjectMapper objectMapper, RestClientStats stats, RestResponseCache cache)
        {
            this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");
            this.stats = requireNonNull(stats, "stats is null");
            this.cache = requireNonNull(cache, "cache is null");
        }

        @Override
        public RESTClient create(HTTPClient.Builder builder, Map<String, String> config)
        {
            builder.withObjectMapper(objectMapper);
            return new CachingRestClient(builder.build(), cache, stats, config);
        }

        @Override
        public RestClientStats stats()
        {
            return stats;
        }
    }

    private static class NonCachingRestClientFactory
            implements RestClientFactory
    {
        private final RestClientStats stats;

        NonCachingRestClientFactory(RestClientStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public RESTClient create(HTTPClient.Builder builder, Map<String, String> config)
        {
            return builder.build();
        }

        @Override
        public RestClientStats stats()
        {
            return stats;
        }
    }

    private static class RestResponseCacheExporter
    {
        private final Optional<MBeanExport> export;

        @Inject
        public RestResponseCacheExporter(MBeanExporter exporter, Optional<RestResponseCache> restResponseCache)
        {
            this.export = restResponseCache
                    .map(cache -> exporter.exportWithGeneratedName(cache, RestResponseCache.class));
        }

        @PreDestroy
        public void unexport()
        {
            export.ifPresent(MBeanExport::unexport);
        }
    }
}
