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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.plugin.iceberg.catalog.rest.RestResponseCache.CachedResponse;
import io.trino.plugin.iceberg.catalog.rest.RestResponseCache.ObjectType;
import jakarta.annotation.Nullable;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.rest.ParserContext;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterViewRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadTableResponseParser;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.LoadViewResponseParser;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.PropertyUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.iceberg.catalog.rest.RestResponseCache.ObjectType.TABLE;
import static io.trino.plugin.iceberg.catalog.rest.RestResponseCache.ObjectType.VIEW;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.rest.RESTCatalogProperties.NAMESPACE_SEPARATOR;
import static org.apache.iceberg.rest.RESTCatalogProperties.NAMESPACE_SEPARATOR_DEFAULT;

class CachingRestClient
        implements RESTClient
{
    private static final Logger log = Logger.get(CachingRestClient.class);

    private static final Consumer<Map<String, String>> IGNORE_RESPONSE_HEADERS = _ -> {};
    private static final String TABLES_PATH_SEGMENT = "tables";
    private static final String VIEWS_PATH_SEGMENT = "views";
    private static final String REGISTER_TABLE_PATH_SEGMENT = "register";
    private static final String REGISTER_VIEW_PATH_SEGMENT = "register-view";
    private static final String METRICS_PATH_SEGMENT = "metrics";
    private static final String PLAN_PATH_SEGMENT = "plan";
    private static final String TASKS_PATH_SEGMENT = "tasks";
    private static final Set<String> KNOWN_LOAD_TABLE_QUERY_PARAMETERS = ImmutableSet.of("snapshots", "referenced-by");
    private static final Set<String> KNOWN_LOAD_VIEW_QUERY_PARAMETERS = ImmutableSet.of("referenced-by");

    private final RESTClient delegate;
    private final RestResponseCache cache;
    private final RestClientStats stats;
    private final String namespaceSeparator;
    private final ResourcePaths resourcePaths;

    public CachingRestClient(
            RESTClient delegate,
            RestResponseCache cache,
            RestClientStats stats,
            Map<String, String> config)
    {
        this(delegate, cache, stats, namespaceSeparator(config), ResourcePaths.forCatalogProperties(requireNonNull(config, "config is null")));
    }

    private CachingRestClient(
            RESTClient delegate,
            RestResponseCache cache,
            RestClientStats stats,
            String namespaceSeparator,
            ResourcePaths resourcePaths)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.cache = requireNonNull(cache, "cache is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.namespaceSeparator = requireNonNull(namespaceSeparator, "namespaceSeparator is null");
        this.resourcePaths = requireNonNull(resourcePaths, "resourcePaths is null");
    }

    private static String namespaceSeparator(Map<String, String> config)
    {
        return PropertyUtil.propertyAsString(
                requireNonNull(config, "config is null"),
                NAMESPACE_SEPARATOR,
                NAMESPACE_SEPARATOR_DEFAULT);
    }

    @Override
    public void head(String path, Supplier<Map<String, String>> headers, Consumer<ErrorResponse> errorHandler)
    {
        delegate.head(path, headers, errorHandler);
    }

    @Override
    public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
    {
        delegate.head(path, headers, errorHandler);
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Supplier<Map<String, String>> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        try {
            return delegate.delete(path, queryParams, responseType, headers, errorHandler);
        }
        finally {
            invalidate(path, Optional.empty());
        }
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path,
            Class<T> responseType,
            Supplier<Map<String, String>> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        try {
            return delegate.delete(path, responseType, headers, errorHandler);
        }
        finally {
            invalidate(path, Optional.empty());
        }
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        try {
            return delegate.delete(path, responseType, headers, errorHandler);
        }
        finally {
            invalidate(path, Optional.empty());
        }
    }

    @Override
    public <T extends RESTResponse> T delete(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        try {
            return delegate.delete(path, queryParams, responseType, headers, errorHandler);
        }
        finally {
            invalidate(path, Optional.empty());
        }
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Class<T> responseType,
            Supplier<Map<String, String>> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return get(path, responseType, headers.get(), errorHandler);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return switch (resolveCacheableResource(path, responseType, ImmutableMap.of(), headers)) {
            case TableResource table -> loadAndCacheTable(table, ImmutableMap.of(), headers, errorHandler, IGNORE_RESPONSE_HEADERS);
            case ViewResource view -> loadAndCacheView(view, ImmutableMap.of(), headers, errorHandler, IGNORE_RESPONSE_HEADERS);
            default -> delegate.get(path, responseType, headers, errorHandler);
        };
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Supplier<Map<String, String>> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return get(path, queryParams, responseType, headers.get(), errorHandler, IGNORE_RESPONSE_HEADERS);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            ParserContext parserContext)
    {
        // Cached raw JSON cannot be replayed with the caller's ParserContext, which may produce
        // non-equivalent deserialization. If this overload is later made cacheable, both miss and
        // hit parsing must use equivalent parser-context-aware deserialization.
        Resource resource = resolveCacheableResource(path, responseType, queryParams, headers);
        recordInvocation(resource);
        return delegate.get(path, queryParams, responseType, headers, errorHandler, parserContext);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Supplier<Map<String, String>> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        return get(path, queryParams, responseType, headers.get(), errorHandler, responseHeaders);
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        return switch (resolveCacheableResource(path, responseType, queryParams, headers)) {
            case TableResource table -> loadAndCacheTable(table, queryParams, headers, errorHandler, responseHeaders);
            case ViewResource view -> loadAndCacheView(view, queryParams, headers, errorHandler, responseHeaders);
            default -> delegate.get(path, queryParams, responseType, headers, errorHandler, responseHeaders);
        };
    }

    @Override
    public <T extends RESTResponse> T get(
            String path,
            Map<String, String> queryParams,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        return get(path, queryParams, responseType, headers, errorHandler, IGNORE_RESPONSE_HEADERS);
    }

    private <T extends RESTResponse> @Nullable T loadAndCacheTable(
            TableResource resource,
            @Nullable Map<String, String> queryParams,
            @Nullable Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        LoadTableResponse loadTableResponse = getCachedResponse(
                resource,
                queryParams,
                headers,
                errorHandler,
                responseHeaders,
                LoadTableResponseParser::fromJson);
        @SuppressWarnings("unchecked")
        T result = (T) loadTableResponse;
        return result;
    }

    private <T extends RESTResponse> @Nullable T loadAndCacheView(
            ViewResource resource,
            @Nullable Map<String, String> queryParams,
            @Nullable Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        LoadViewResponse loadViewResponse = getCachedResponse(
                resource,
                queryParams,
                headers,
                errorHandler,
                responseHeaders,
                LoadViewResponseParser::fromJson);
        @SuppressWarnings("unchecked")
        T result = (T) loadViewResponse;
        return result;
    }

    private <T extends RESTResponse> @Nullable T getCachedResponse(
            CacheableResource resource,
            @Nullable Map<String, String> queryParams,
            @Nullable Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders,
            Function<String, T> deserializer)
    {
        RestResponseCache.QueryParams params = RestResponseCache.QueryParams.from(queryParams);
        // Hand back an empty header map on both hits and misses. A cached response retains only the
        // JSON body, not the delegate's response headers, so a miss must not surface headers that a
        // later hit could not reproduce -- otherwise callers would observe hit/miss-dependent behavior.
        // The only response header the load-table API defines is ETag, which is optional and
        // unused here; discarding it (and anything else) avoids leaking a header unintentionally.
        EmptyResponseHeadersEmitter responseHeadersEmitter = new EmptyResponseHeadersEmitter(responseHeaders);
        try {
            AtomicReference<T> result = new AtomicReference<>();
            Optional<CachedResponse> cached = cache.get(resource.type(), resource.identifier(), params, () -> {
                recordInvocation(resource);
                RawResponse response = delegate.get(resource.path(), queryParams, RawResponse.class, headers, errorHandler, responseHeadersEmitter);
                if (response == null) {
                    // The delegate can legitimately return null (e.g. a 304 Not Modified). A null must never be cached.
                    throw new NullResponseException();
                }
                // Deserialize while loading to avoid caching malformed responses.
                T deserialized = deserializer.apply(response.json());
                checkState(deserialized != null, "deserialized response is null");
                result.set(deserialized);
                return response;
            });

            responseHeadersEmitter.ensureEmitted();
            if (cached.isEmpty()) {
                ErrorResponse notFound = ErrorResponse.builder()
                        .responseCode(404)
                        .withType(resource.notFoundErrorType())
                        .withMessage(resource.notFoundErrorMessage())
                        .build();
                errorHandler.accept(notFound);
                throw new RESTException("Unhandled error: %s", notFound); // normally, the error handler should throw
            }
            else if (result.get() != null) {
                return result.get();
            }
            else {
                return deserializer.apply(cached.get().json());
            }
        }
        catch (NullResponseException e) {
            return null;
        }
    }

    private static class EmptyResponseHeadersEmitter
            implements Consumer<Map<String, String>>
    {
        private final Consumer<Map<String, String>> downstream;
        private final AtomicBoolean emitted = new AtomicBoolean();

        public EmptyResponseHeadersEmitter(Consumer<Map<String, String>> downstream)
        {
            this.downstream = requireNonNull(downstream, "downstream is null");
        }

        @Override
        public void accept(Map<String, String> stringStringMap)
        {
            emitted.set(true);
            emitEmpty();
        }

        public void ensureEmitted()
        {
            if (emitted.compareAndSet(false, true)) {
                emitEmpty();
            }
        }

        private void emitEmpty()
        {
            downstream.accept(ImmutableMap.of());
        }
    }

    private static class NullResponseException
            extends RuntimeException
    {
        public NullResponseException()
        {
            // This exception is a sentinel used only to signal a cache miss to the enclosing catch;
            // it never escapes and is never logged, so the stack trace is pointless overhead and is suppressed.
            super(null, null, false, false);
        }
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            @Nullable RESTRequest body,
            Class<T> responseType,
            Supplier<Map<String, String>> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        try {
            return delegate.post(path, body, responseType, headers, errorHandler);
        }
        finally {
            invalidate(path, Optional.ofNullable(body));
        }
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            @Nullable RESTRequest body,
            Class<T> responseType,
            Supplier<Map<String, String>> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        try {
            return delegate.post(path, body, responseType, headers, errorHandler, responseHeaders);
        }
        finally {
            invalidate(path, Optional.ofNullable(body));
        }
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            @Nullable RESTRequest body,
            Class<T> responseType,
            @Nullable Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders,
            ParserContext parserContext)
    {
        try {
            return delegate.post(path, body, responseType, headers, errorHandler, responseHeaders, parserContext);
        }
        finally {
            invalidate(path, Optional.ofNullable(body));
        }
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            @Nullable RESTRequest body,
            Class<T> responseType,
            @Nullable Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler,
            Consumer<Map<String, String>> responseHeaders)
    {
        try {
            return delegate.post(path, body, responseType, headers, errorHandler, responseHeaders);
        }
        finally {
            invalidate(path, Optional.ofNullable(body));
        }
    }

    @Override
    public <T extends RESTResponse> T post(
            String path,
            @Nullable RESTRequest body,
            Class<T> responseType,
            @Nullable Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        try {
            return delegate.post(path, body, responseType, headers, errorHandler);
        }
        finally {
            invalidate(path, Optional.ofNullable(body));
        }
    }

    @Override
    public <T extends RESTResponse> T postForm(
            String path,
            Map<String, String> formData,
            Class<T> responseType,
            Supplier<Map<String, String>> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        try {
            return delegate.postForm(path, formData, responseType, headers, errorHandler);
        }
        finally {
            if (responseType != OAuthTokenResponse.class) {
                cache.invalidateAll();
            }
        }
    }

    @Override
    public <T extends RESTResponse> T postForm(
            String path,
            Map<String, String> formData,
            Class<T> responseType,
            Map<String, String> headers,
            Consumer<ErrorResponse> errorHandler)
    {
        try {
            return delegate.postForm(path, formData, responseType, headers, errorHandler);
        }
        finally {
            if (responseType != OAuthTokenResponse.class) {
                cache.invalidateAll();
            }
        }
    }

    @Override
    public RESTClient withAuthSession(AuthSession session)
    {
        return new CachingRestClient(delegate.withAuthSession(session), cache, stats, namespaceSeparator, resourcePaths);
    }

    @Override
    public void close()
            throws IOException
    {
        delegate.close();
    }

    private void invalidate(String path, Optional<RESTRequest> body)
    {
        switch (resolveResource(path, body)) {
            case TableResource table -> cache.invalidate(table.identifier());
            case ViewResource view -> cache.invalidate(view.identifier());
            case CreateResource create -> cache.invalidate(create.identifier());
            case RegisterResource register -> cache.invalidate(register.identifier());
            case RenameResource rename -> {
                cache.invalidate(rename.source());
                cache.invalidate(rename.destination());
            }
            case TableMetricsResource _, TableScanResource _ -> {
                // ignore
            }
            default -> {
                log.debug("Unknown resource: %s. Invalidating the entire cache.", path);
                cache.invalidateAll();
            }
        }
    }

    private Resource resolveCacheableResource(
            String path,
            Class<?> responseType,
            @Nullable Map<String, String> queryParams,
            @Nullable Map<String, String> headers)
    {
        switch (resolveResource(path, Optional.empty())) {
            case TableResource table -> {
                if (isCacheable(responseType, LoadTableResponse.class, queryParams, KNOWN_LOAD_TABLE_QUERY_PARAMETERS, headers)) {
                    return table;
                }
            }
            case ViewResource view -> {
                if (isCacheable(responseType, LoadViewResponse.class, queryParams, KNOWN_LOAD_VIEW_QUERY_PARAMETERS, headers)) {
                    return view;
                }
            }
            default -> {
                return new UnknownResource(path);
            }
        }
        return new UnknownResource(path);
    }

    private static boolean isCacheable(
            Class<?> responseType,
            Class<?> knownResponseType,
            @Nullable Map<String, String> queryParams,
            Set<String> knownQueryParameters,
            @Nullable Map<String, String> headers)
    {
        return responseType == knownResponseType
                && (queryParams == null || knownQueryParameters.containsAll(queryParams.keySet()))
                && (headers == null || headers.isEmpty());
    }

    private Resource resolveResource(String path, Optional<RESTRequest> body)
    {
        Optional<String> rawPath = rawPath(path);
        if (rawPath.isEmpty()) {
            return new UnknownResource(path);
        }
        String resourcePath = rawPath.get();
        if (body.isPresent() && body.get() instanceof RenameTableRequest renameRequest) {
            if (resourcePaths.rename().equals(resourcePath) || resourcePaths.renameView().equals(resourcePath)) {
                // v1/{prefix}/tables/rename
                // v1/{prefix}/views/rename
                return new RenameResource(path, renameRequest.source(), renameRequest.destination());
            }
        }
        // ResourcePaths.namespaces() renders v1/{prefix}/namespaces. Matching that anchor first
        // avoids guessing which "namespaces" segment belongs destination the catalog prefix.
        String namespacesPathPrefix = resourcePaths.namespaces() + "/";
        if (resourcePath.startsWith(namespacesPathPrefix)) {
            return resolveNamespacedResource(path, splitIntoSegments(resourcePath.substring(namespacesPathPrefix.length())), body);
        }
        return new UnknownResource(path);
    }

    private Resource resolveNamespacedResource(String path, List<String> tail, Optional<RESTRequest> body)
    {
        if (tail.size() < 2) {
            return new UnknownResource(path);
        }

        RESTRequest request = body.orElse(null);
        String namespace = tail.getFirst();
        List<String> rest = tail.subList(2, tail.size());
        return switch (tail.get(1)) {
            case TABLES_PATH_SEGMENT -> {
                if (rest.size() == 1) {
                    // v1/{prefix}/namespaces/{namespace}/tables/{name}
                    yield new TableResource(path, toIdentifier(namespace, rest.getFirst()));
                }
                if (rest.size() == 2 && rest.get(1).equals(METRICS_PATH_SEGMENT)) {
                    // v1/{prefix}/namespaces/{namespace}/tables/{name}/metrics
                    yield new TableMetricsResource(path);
                }
                if (rest.size() >= 2 && rest.get(1).equals(PLAN_PATH_SEGMENT)) {
                    // v1/{prefix}/namespaces/{namespace}/tables/{name}/plan
                    // v1/{prefix}/namespaces/{namespace}/tables/{name}/plan/{plan-id}
                    yield new TableScanResource(path);
                }
                if (rest.size() == 2 && rest.get(1).equals(TASKS_PATH_SEGMENT)) {
                    // v1/{prefix}/namespaces/{namespace}/tables/{table}/tasks
                    yield new TableScanResource(path);
                }
                if (rest.isEmpty() && request instanceof CreateTableRequest createTable) {
                    // v1/{prefix}/namespaces/{namespace}/tables
                    yield new CreateResource(path, toIdentifier(namespace, createTable.name()));
                }
                yield new UnknownResource(path);
            }
            case VIEWS_PATH_SEGMENT -> {
                if (rest.size() == 1) {
                    // v1/{prefix}/namespaces/{namespace}/views/{name}
                    yield new ViewResource(path, toIdentifier(namespace, rest.getFirst()));
                }
                if (rest.isEmpty() && request instanceof CreateViewRequest createView) {
                    // v1/{prefix}/namespaces/{namespace}/views
                    yield new CreateResource(path, toIdentifier(namespace, createView.name()));
                }
                yield new UnknownResource(path);
            }
            case REGISTER_TABLE_PATH_SEGMENT -> {
                if (rest.isEmpty() && request instanceof RegisterTableRequest registerTable) {
                    // v1/{prefix}/namespaces/{namespace}/register
                    yield new RegisterResource(path, toIdentifier(namespace, registerTable.name()));
                }
                yield new UnknownResource(path);
            }
            case REGISTER_VIEW_PATH_SEGMENT -> {
                if (rest.isEmpty() && request instanceof RegisterViewRequest registerView) {
                    // v1/{prefix}/namespaces/{namespace}/register-view
                    yield new RegisterResource(path, toIdentifier(namespace, registerView.name()));
                }
                yield new UnknownResource(path);
            }
            default -> new UnknownResource(path);
        };
    }

    private TableIdentifier toIdentifier(String namespace, String segment)
    {
        return TableIdentifier.of(RESTUtil.decodeNamespace(namespace, namespaceSeparator), RESTUtil.decodeString(segment));
    }

    private static Optional<String> rawPath(String path)
    {
        URI uri;
        try {
            uri = new URI(path);
        }
        catch (URISyntaxException _) {
            return Optional.empty();
        }
        if (uri.getScheme() != null || uri.getRawAuthority() != null || uri.getRawQuery() != null || uri.getRawFragment() != null) {
            return Optional.empty();
        }
        // Split the raw, still-encoded path. Identifier segments are decoded exactly once, by
        // readIdentifier via RESTUtil, mirroring how the server encodes them. Decoding here instead
        // (for example via URI.getPath()) would decode a second time and corrupt or reject identifiers
        // that contain encoded characters (for example a table named "a%b", sent as "a%25b").
        String rawPath = uri.getRawPath();
        if (rawPath.startsWith("/")) {
            return Optional.empty();
        }
        return Optional.of(rawPath);
    }

    private static List<String> splitIntoSegments(String path)
    {
        return Splitter.on('/').splitToList(path);
    }

    private void recordInvocation(Resource resource)
    {
        switch (resource) {
            case TableResource _ -> stats.recordLoadTable();
            case ViewResource _ -> stats.recordLoadView();
            default -> {
                // ignore
            }
        }
    }

    private sealed interface Resource
    {
        String path();
    }

    private sealed interface CacheableResource
            extends Resource
    {
        TableIdentifier identifier();

        ObjectType type();

        String notFoundErrorType();

        String notFoundErrorMessage();
    }

    private record UnknownResource(String path)
            implements Resource
    {
        public UnknownResource
        {
            requireNonNull(path, "path is null");
        }
    }

    private record TableResource(String path, TableIdentifier identifier)
            implements CacheableResource
    {
        public TableResource
        {
            requireNonNull(path, "path is null");
            requireNonNull(identifier, "identifier is null");
        }

        @Override
        public ObjectType type()
        {
            return TABLE;
        }

        @Override
        public String notFoundErrorType()
        {
            return NoSuchTableException.class.getSimpleName();
        }

        @Override
        public String notFoundErrorMessage()
        {
            return "Table does not exist: %s".formatted(identifier);
        }
    }

    private record ViewResource(String path, TableIdentifier identifier)
            implements CacheableResource
    {
        public ViewResource
        {
            requireNonNull(path, "path is null");
            requireNonNull(identifier, "identifier is null");
        }

        @Override
        public ObjectType type()
        {
            return VIEW;
        }

        @Override
        public String notFoundErrorType()
        {
            return NoSuchViewException.class.getSimpleName();
        }

        @Override
        public String notFoundErrorMessage()
        {
            return "View does not exist: %s".formatted(identifier);
        }
    }

    private record CreateResource(String path, TableIdentifier identifier)
            implements Resource
    {
        public CreateResource
        {
            requireNonNull(path, "path is null");
            requireNonNull(identifier, "identifier is null");
        }
    }

    private record RegisterResource(String path, TableIdentifier identifier)
            implements Resource
    {
        public RegisterResource
        {
            requireNonNull(path, "path is null");
            requireNonNull(identifier, "identifier is null");
        }
    }

    private record RenameResource(String path, TableIdentifier source, TableIdentifier destination)
            implements Resource
    {
        public RenameResource
        {
            requireNonNull(path, "path is null");
            requireNonNull(source, "source is null");
            requireNonNull(destination, "destination is null");
        }
    }

    private record TableMetricsResource(String path)
            implements Resource
    {
        public TableMetricsResource
        {
            requireNonNull(path, "path is null");
        }
    }

    private record TableScanResource(String path)
            implements Resource
    {
        public TableScanResource
        {
            requireNonNull(path, "path is null");
        }
    }
}
