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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.HTTPRequest.HTTPMethod;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.iceberg.rest.RestCatalogEndpoints.assertEndpointValid;
import static org.apache.iceberg.rest.RestCatalogEndpoints.commitTransactionRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.createNamespaceRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.createTableRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.createViewRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.fetchScanTasksRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.getIcebergEndpoints;
import static org.apache.iceberg.rest.RestCatalogEndpoints.planTableScanRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.registerTableRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.registerViewRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.renameRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.reportMetricsRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.updateNamespacePropertiesRequest;
import static org.apache.iceberg.rest.RestCatalogEndpoints.updateTableRequest;
import static org.assertj.core.api.Assertions.assertThat;

final class TestCachingRestClient
{
    private static final List<String> PREFIXES = Arrays.asList(null, "catalog", "nested/prefix", "nested/namespaces/prefix");
    private static final InvalidateAll INVALIDATE_ALL = new InvalidateAll();

    private static final Set<Endpoint> KNOWN_ENDPOINTS = ImmutableSet.of(
            // Cached
            Endpoint.V1_LOAD_TABLE,
            Endpoint.V1_LOAD_VIEW,
            // Non-cached
            Endpoint.V1_LIST_NAMESPACES,
            Endpoint.V1_LOAD_NAMESPACE,
            Endpoint.V1_NAMESPACE_EXISTS,
            Endpoint.V1_LIST_TABLES,
            Endpoint.V1_TABLE_EXISTS,
            Endpoint.V1_TABLE_CREDENTIALS,
            Endpoint.V1_LIST_VIEWS,
            Endpoint.V1_VIEW_EXISTS,
            Endpoint.V1_FETCH_TABLE_SCAN_PLAN,
            // Invalidating specific entries
            Endpoint.V1_UPDATE_TABLE,
            Endpoint.V1_DELETE_TABLE,
            Endpoint.V1_UPDATE_VIEW,
            Endpoint.V1_DELETE_VIEW,
            Endpoint.V1_CREATE_TABLE,
            Endpoint.V1_REGISTER_TABLE,
            Endpoint.V1_CREATE_VIEW,
            Endpoint.V1_REGISTER_VIEW,
            Endpoint.V1_RENAME_TABLE,
            Endpoint.V1_RENAME_VIEW,
            // Invalidating the entire cache
            Endpoint.V1_CREATE_NAMESPACE,
            Endpoint.V1_UPDATE_NAMESPACE,
            Endpoint.V1_DELETE_NAMESPACE,
            Endpoint.V1_COMMIT_TRANSACTION,
            Endpoint.V1_TABLE_REMOTE_SIGN,
            Endpoint.V1_SUBMIT_TABLE_SCAN_PLAN,
            Endpoint.V1_CANCEL_TABLE_SCAN_PLAN,
            Endpoint.V1_FETCH_TABLE_SCAN_PLAN_TASKS,
            // Ignored during invalidation
            Endpoint.V1_REPORT_METRICS);

    @Test
    void testEverythingImplemented()
    {
        assertAllMethodsOverridden(RESTClient.class, CachingRestClient.class);
    }

    @Test
    void testAllIcebergEndpointsHaveExplicitCachePolicy()
    {
        // Unknown mutation endpoints are handled safely by invalidating the full cache. Still, every
        // Iceberg Endpoint constant should be listed here so an Iceberg upgrade forces an explicit
        // cache-policy decision and matching behavior coverage.
        // This is also a cache-opportunity check: a new read/load endpoint should not silently
        // remain uncached just because CachingRestClient does not recognize it yet.
        Set<Endpoint> icebergEndpoints = getIcebergEndpoints();

        assertThat(icebergEndpoints)
                .as("Iceberg REST endpoints changed; review CachingRestClient cache policy")
                .containsExactlyInAnyOrderElementsOf(KNOWN_ENDPOINTS);
    }

    @Test
    void testCacheableReads()
    {
        for (String prefix : PREFIXES) {
            Map<String, String> properties = prefixProperties(prefix);
            ResourcePaths paths = ResourcePaths.forCatalogProperties(properties);
            Namespace namespace = Namespace.of("default");
            TableIdentifier table = TableIdentifier.of(namespace, "orders");
            TableIdentifier view = TableIdentifier.of(namespace, "report");
            Namespace caseSensitiveNamespace = Namespace.of("Default", "CaseSensitive");
            TableIdentifier caseSensitiveTable = TableIdentifier.of(caseSensitiveNamespace, "Orders");
            TableIdentifier caseSensitiveView = TableIdentifier.of(caseSensitiveNamespace, "Report");

            for (TableIdentifier tableIdentifier : ImmutableList.of(table, caseSensitiveTable)) {
                assertInteraction(
                        properties,
                        client -> client.get(paths.table(tableIdentifier), LoadTableResponse.class, ImmutableMap.of(), _ -> {}),
                        getFromCache(tableIdentifier));
                assertInteraction(
                        properties,
                        client -> client.get(paths.table(tableIdentifier), ImmutableMap.of("snapshots", "all"), LoadTableResponse.class, ImmutableMap.of(), _ -> {}),
                        getFromCache(tableIdentifier));
            }

            for (TableIdentifier viewIdentifier : ImmutableList.of(view, caseSensitiveView)) {
                assertInteraction(properties, client -> client.get(paths.view(viewIdentifier), LoadViewResponse.class, ImmutableMap.of(), _ -> {}), getFromCache(viewIdentifier));
                assertInteraction(
                        properties,
                        client -> client.get(paths.view(viewIdentifier), ImmutableMap.of("referenced-by", "true"), LoadViewResponse.class, ImmutableMap.of(), _ -> {}),
                        getFromCache(viewIdentifier));
            }
        }

        Map<String, String> properties = prefixProperties("catalog");
        ResourcePaths paths = ResourcePaths.forCatalogProperties(properties);
        TableIdentifier nestedTable = TableIdentifier.of(Namespace.of("a", "b", "c"), "orders");
        TableIdentifier percentTable = TableIdentifier.of(Namespace.of("default"), "orders%");
        TableIdentifier slashTable = TableIdentifier.of(Namespace.of("default"), "order/part");
        TableIdentifier unicodeTable = TableIdentifier.of(Namespace.of("with space", "dève"), "táblë?");

        for (TableIdentifier tableIdentifier : ImmutableList.of(nestedTable, percentTable, slashTable, unicodeTable)) {
            assertInteraction(
                    properties,
                    client -> client.get(paths.table(tableIdentifier), LoadTableResponse.class, ImmutableMap.of(), _ -> {}),
                    getFromCache(tableIdentifier));
        }
    }

    @Test
    void testNonCacheableReadsDoNotTouchCache()
    {
        Map<String, String> properties = prefixProperties("nested/namespaces/prefix");
        ResourcePaths paths = ResourcePaths.forCatalogProperties(properties);
        Namespace namespace = Namespace.of("default");
        TableIdentifier table = TableIdentifier.of(namespace, "orders");

        assertInteraction(properties, client -> client.get(paths.namespaces(), ListNamespacesResponse.class, ImmutableMap.of(), _ -> {}));
        assertInteraction(properties, client -> client.get(paths.namespace(namespace), GetNamespaceResponse.class, ImmutableMap.of(), _ -> {}));
        assertInteraction(properties, client -> client.head(paths.namespace(namespace), ImmutableMap.of(), _ -> {}));
        assertInteraction(properties, client -> client.get(paths.tables(namespace), ListTablesResponse.class, ImmutableMap.of(), _ -> {}));
        assertInteraction(properties, client -> client.head(paths.table(table), ImmutableMap.of(), _ -> {}));
        assertInteraction(properties, client -> client.get(paths.views(namespace), ListTablesResponse.class, ImmutableMap.of(), _ -> {}));
        assertInteraction(properties, client -> client.get(paths.table(table), LoadTableResponse.class, ImmutableMap.of("header", "value"), _ -> {}));
        assertInteraction(properties, client -> client.get(paths.table(table) + "/credentials", LoadCredentialsResponse.class, ImmutableMap.of(), _ -> {}));
        assertInteraction(properties, client -> client.get(paths.plan(table, "planId"), FetchPlanningResultResponse.class, ImmutableMap.of(), _ -> {}));
    }

    @Test
    void testSingleEntryInvalidations()
    {
        for (String prefix : PREFIXES) {
            Map<String, String> properties = prefixProperties(prefix);
            ResourcePaths paths = ResourcePaths.forCatalogProperties(properties);
            Namespace namespace = Namespace.of("default");
            TableIdentifier table = TableIdentifier.of(namespace, "orders");
            TableIdentifier view = TableIdentifier.of(namespace, "report");
            Namespace caseSensitiveNamespace = Namespace.of("Default", "CaseSensitive");
            TableIdentifier caseSensitiveTable = TableIdentifier.of(caseSensitiveNamespace, "Orders");
            TableIdentifier caseSensitiveView = TableIdentifier.of(caseSensitiveNamespace, "Report");

            for (TableIdentifier tableIdentifier : ImmutableList.of(table, caseSensitiveTable)) {
                assertInteraction(
                        properties,
                        client -> client.post(paths.table(tableIdentifier), updateTableRequest(), LoadTableResponse.class, ImmutableMap.of(), _ -> {}),
                        invalidateKey(tableIdentifier));
                assertInteraction(
                        properties,
                        client -> client.delete(paths.table(tableIdentifier), LoadTableResponse.class, ImmutableMap.of(), _ -> {}),
                        invalidateKey(tableIdentifier));
                assertInteraction(
                        properties,
                        client -> client.post(paths.tables(tableIdentifier.namespace()), createTableRequest(tableIdentifier.name()), LoadTableResponse.class, ImmutableMap.of(), _ -> {}),
                        invalidateKey(tableIdentifier));
                assertInteraction(
                        properties,
                        client -> client.post(paths.register(tableIdentifier.namespace()), registerTableRequest(tableIdentifier.name()), LoadTableResponse.class, ImmutableMap.of(), _ -> {}),
                        invalidateKey(tableIdentifier));
                assertInteraction(
                        properties,
                        client -> client.post(paths.rename(), renameRequest(tableIdentifier, renamed(tableIdentifier)), LoadTableResponse.class, ImmutableMap.of(), _ -> {}),
                        invalidateKey(tableIdentifier),
                        invalidateKey(renamed(tableIdentifier)));
            }

            for (TableIdentifier viewIdentifier : ImmutableList.of(view, caseSensitiveView)) {
                assertInteraction(
                        properties,
                        client -> client.post(paths.view(viewIdentifier), updateTableRequest(), LoadViewResponse.class, ImmutableMap.of(), _ -> {}),
                        invalidateKey(viewIdentifier));
                assertInteraction(properties, client -> client.delete(paths.view(viewIdentifier), LoadViewResponse.class, ImmutableMap.of(), _ -> {}), invalidateKey(viewIdentifier));
                assertInteraction(
                        properties,
                        client -> client.post(paths.views(viewIdentifier.namespace()), createViewRequest(viewIdentifier.name()), LoadViewResponse.class, ImmutableMap.of(), _ -> {}),
                        invalidateKey(viewIdentifier));
                assertInteraction(
                        properties,
                        client -> client.post(paths.registerView(viewIdentifier.namespace()), registerViewRequest(viewIdentifier.name()), LoadViewResponse.class, ImmutableMap.of(), _ -> {}),
                        invalidateKey(viewIdentifier));
                assertInteraction(
                        properties,
                        client -> client.post(paths.renameView(), renameRequest(viewIdentifier, renamed(viewIdentifier)), LoadViewResponse.class, ImmutableMap.of(), _ -> {}),
                        invalidateKey(viewIdentifier),
                        invalidateKey(renamed(viewIdentifier)));
            }
        }
    }

    private static TableIdentifier renamed(TableIdentifier original)
    {
        String[] renamedLevels = original.namespace().levels().clone();
        renamedLevels[0] = renamedLevels[0] + "_renamed";
        return TableIdentifier.of(Namespace.of(renamedLevels), original.name() + "_renamed");
    }

    @Test
    void testIgnoredMutations()
    {
        for (String prefix : PREFIXES) {
            Map<String, String> properties = prefixProperties(prefix);
            ResourcePaths paths = ResourcePaths.forCatalogProperties(properties);
            TableIdentifier table = TableIdentifier.of(Namespace.of("default"), "orders");
            TableIdentifier caseSensitiveTable = TableIdentifier.of(Namespace.of("Default", "CaseSensitive"), "Orders");

            for (TableIdentifier tableIdentifier : ImmutableList.of(table, caseSensitiveTable)) {
                assertInteraction(properties, client -> client.post(paths.metrics(tableIdentifier), reportMetricsRequest(), null, ImmutableMap.of(), _ -> {}));
            }
        }

        Map<String, String> properties = prefixProperties("catalog");
        ResourcePaths paths = ResourcePaths.forCatalogProperties(properties);
        TableIdentifier encodedTable = TableIdentifier.of(Namespace.of("a/b"), "orders%");
        assertInteraction(properties, client -> client.post(paths.metrics(encodedTable), reportMetricsRequest(), null, ImmutableMap.of(), _ -> {}));
    }

    @Test
    void testUnknownMutationsInvalidateAll()
    {
        Map<String, String> properties = prefixProperties("nested/namespaces/prefix");
        ResourcePaths paths = ResourcePaths.forCatalogProperties(properties);
        Namespace namespace = Namespace.of("default");
        TableIdentifier source = TableIdentifier.of(namespace, "source");

        assertInteraction(properties, client -> client.post(paths.namespaces(), createNamespaceRequest(), CreateNamespaceResponse.class, ImmutableMap.of(), _ -> {}), INVALIDATE_ALL);
        assertInteraction(
                properties,
                client -> client.post(paths.namespaceProperties(namespace), updateNamespacePropertiesRequest(), UpdateNamespacePropertiesResponse.class, ImmutableMap.of(), _ -> {}),
                INVALIDATE_ALL);
        assertInteraction(properties, client -> client.delete(paths.namespace(namespace), LoadTableResponse.class, ImmutableMap.of(), _ -> {}), INVALIDATE_ALL);
        assertInteraction(
                properties,
                client -> client.post(paths.commitTransaction(), commitTransactionRequest(), LoadTableResponse.class, ImmutableMap.of(), _ -> {}),
                INVALIDATE_ALL);
        assertInteraction(properties, client -> client.post(paths.remoteSign(source), null, RemoteSignResponse.class, ImmutableMap.of(), _ -> {}), INVALIDATE_ALL);
        assertInteraction(
                properties,
                client -> client.post(paths.planTableScan(source), planTableScanRequest(), PlanTableScanResponse.class, ImmutableMap.of(), _ -> {}),
                INVALIDATE_ALL);
        assertInteraction(
                properties,
                client -> client.delete(paths.plan(source, "planId"), null, ImmutableMap.of(), _ -> {}),
                INVALIDATE_ALL);
        assertInteraction(
                properties,
                client -> client.post(paths.fetchScanTasks(source), fetchScanTasksRequest(), FetchScanTasksResponse.class, ImmutableMap.of(), _ -> {}),
                INVALIDATE_ALL);
    }

    @Test
    void testUnrecognizedMutationPathsInvalidateAll()
    {
        RESTClient nonValidatingClient = new TestingRestClient(ImmutableMap.of(), false);
        ImmutableList.of(
                        // Empty, root, and truncated paths.
                        "",
                        "/",
                        "v1",
                        // A leading slash is never part of a ResourcePaths-built path and is not accepted.
                        "/v1/namespaces/default/tables/orders",
                        // Wrong version, segment ordering, and casing (keywords are matched literally).
                        "v2/catalog/namespaces/default/tables/orders",
                        "catalog/v1/namespaces/default/tables/orders",
                        "v1/catalog/Namespaces/default/tables/orders",
                        "v1/catalog/namespaces/default/Tables/orders",
                        // Sub-resources that are not a plain table/view load.
                        "v1/catalog/namespaces/default/tables/orders/credentials",
                        "v1/catalog/namespaces/default/views/report/extra",
                        // Query strings, fragments, absolute/authority URLs (e.g. a configured OAuth
                        // endpoint), and malformed percent-encoding are all rejected.
                        "v1/catalog/namespaces/default/tables/orders?warehouse=test",
                        "v1/catalog/namespaces/default/tables/orders?",
                        "v1/catalog/namespaces/default/tables/orders#section",
                        "https://auth.example.test/oauth/token",
                        "https://catalog.example.test/v1/catalog/namespaces/default/tables/orders",
                        "//catalog.example.test/v1/catalog/namespaces/default/tables/orders",
                        "v1/catalog/namespaces/default/tables/orders%")
                .forEach(path -> assertThat(observeCacheInteraction(nonValidatingClient, ImmutableMap.of(), client -> client.post(path, null, LoadTableResponse.class, ImmutableMap.of(), _ -> {})))
                        .containsExactly(INVALIDATE_ALL));
    }

    @Test
    void testNullDelegateResponseIsNotCached()
    {
        Map<String, String> properties = ImmutableMap.of();
        RESTClient delegate = new TestingRestClient(properties, true);
        CachingRestClient client = new CachingRestClient(
                delegate,
                new RestResponseCache(new Duration(10, MINUTES), DataSize.of(1, MEGABYTE), DataSize.of(1, MEGABYTE)),
                new RestClientStats(),
                ImmutableMap.of());
        String loadTablePath = ResourcePaths.forCatalogProperties(properties)
                .table(TableIdentifier.of(Namespace.of("default"), "orders"));
        assertThat(delegate.get(loadTablePath, RawResponse.class, ImmutableMap.of(), _ -> {})).isNull();
        assertThat(client.get(loadTablePath, RawResponse.class, ImmutableMap.of(), _ -> {})).isNull();
    }

    private static Map<String, String> prefixProperties(String prefix)
    {
        return prefix == null ? ImmutableMap.of() : ImmutableMap.of("prefix", prefix);
    }

    private static void assertInteraction(Map<String, String> config, Consumer<RESTClient> call, CacheInteraction... expected)
    {
        RESTClient delegate = new TestingRestClient(config, true);
        call.accept(delegate); // validate endpoint
        assertThat(observeCacheInteraction(delegate, config, call)).containsExactly(expected);
    }

    private static List<CacheInteraction> observeCacheInteraction(RESTClient delegate, Map<String, String> config, Consumer<RESTClient> call)
    {
        RecordingRestResponseCache recordingCache = new RecordingRestResponseCache();
        CachingRestClient client = new CachingRestClient(delegate, recordingCache, new RestClientStats(), config);
        try {
            call.accept(client);
        }
        catch (IgnorableCacheException _) {
            // ignore
        }
        return recordingCache.getCacheInteractions();
    }

    private static final class RecordingRestResponseCache
            extends RestResponseCache
    {
        private final List<CacheInteraction> cacheInteractions = new ArrayList<>();

        public RecordingRestResponseCache()
        {
            super(new Duration(10, MINUTES), DataSize.of(1, MEGABYTE), DataSize.of(1, MEGABYTE));
        }

        @Override
        public Optional<CachedResponse> get(ObjectType objectType, TableIdentifier identifier, QueryParams params, Supplier<RawResponse> loader)
        {
            cacheInteractions.add(getFromCache(identifier));
            throw new IgnorableCacheException();
        }

        @Override
        public void invalidate(TableIdentifier identifier)
        {
            cacheInteractions.add(invalidateKey(identifier));
        }

        @Override
        public void invalidateAll()
        {
            cacheInteractions.add(INVALIDATE_ALL);
        }

        public List<CacheInteraction> getCacheInteractions()
        {
            return cacheInteractions;
        }
    }

    private static final class IgnorableCacheException
            extends RuntimeException {}

    private static GetFromCache getFromCache(TableIdentifier identifier)
    {
        return new GetFromCache(identifier);
    }

    private static InvalidateKey invalidateKey(TableIdentifier identifier)
    {
        return new InvalidateKey(identifier);
    }

    private sealed interface CacheInteraction {}

    private record GetFromCache(TableIdentifier identifier)
            implements CacheInteraction {}

    private record InvalidateKey(TableIdentifier identifier)
            implements CacheInteraction {}

    private record InvalidateAll()
            implements CacheInteraction {}

    private static class TestingRestClient
            implements RESTClient
    {
        private final String prefix;
        private final boolean validate;

        public TestingRestClient(Map<String, String> config, boolean validate)
        {
            this.prefix = config.getOrDefault("prefix", "");
            this.validate = validate;
        }

        @Override
        public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            if (validate) {
                assertEndpointValid(prefix, HTTPMethod.HEAD, path, Optional.empty(), Optional.empty());
            }
        }

        @Override
        public <T extends RESTResponse> T delete(String path, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            if (validate) {
                assertEndpointValid(prefix, HTTPMethod.DELETE, path, Optional.empty(), Optional.ofNullable(responseType));
            }
            return null;
        }

        @Override
        public <T extends RESTResponse> T get(String path, Map<String, String> queryParams, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            if (validate) {
                assertEndpointValid(prefix, HTTPMethod.GET, path, Optional.empty(), Optional.ofNullable(responseType));
            }
            return null;
        }

        @Override
        public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            if (validate) {
                assertEndpointValid(prefix, HTTPMethod.POST, path, Optional.ofNullable(body), Optional.ofNullable(responseType));
            }
            return null;
        }

        @Override
        public <T extends RESTResponse> T postForm(String path, Map<String, String> formData, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            if (validate) {
                assertEndpointValid(prefix, HTTPMethod.POST, path, Optional.empty(), Optional.ofNullable(responseType));
            }
            return null;
        }

        @Override
        public void close()
        {
            throw new UnsupportedOperationException();
        }
    }
}
