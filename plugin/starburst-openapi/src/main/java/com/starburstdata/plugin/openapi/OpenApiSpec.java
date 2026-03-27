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
package com.starburstdata.plugin.openapi;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.starburstdata.plugin.openapi.authentication.OpenApiAuthenticator;
import com.starburstdata.plugin.openapi.conversions.OpenApiDecoder;
import com.starburstdata.plugin.openapi.pagination.OpenApiPaginationStrategy;
import io.airlift.log.Logger;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starburstdata.plugin.openapi.SpecUtil.getGetOperation;
import static com.starburstdata.plugin.openapi.SpecUtil.getJsonResponseSchema;
import static com.starburstdata.plugin.openapi.SpecUtil.getSuccessfulResponse;
import static com.starburstdata.plugin.openapi.conversions.OpenApiDecoder.ONE_COLUMN_DECODER;
import static com.starburstdata.plugin.openapi.pagination.OpenApiPaginationStrategy.READ_ONCE_STRATEGY;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

public class OpenApiSpec
{
    private static final Logger log = Logger.get(OpenApiSpec.class);

    public static final String SCHEMA_NAME = "default";
    public static final String HTTP_OK = "200";
    public static final String MIME_JSON = "application/json";

    private final Set<ConnectorTableFunction> tableFunctions;
    private final Map<String, OpenApiDecoder> pathToDecoder;
    private final Map<String, OpenApiPaginationStrategy<?>> pathToPaginationStrategy;
    private final Map<String, OpenApiAuthenticator> pathToAuthenticator;

    @Inject
    public OpenApiSpec(OpenApiConfig config)
    {
        this(parse(config.getSpecLocation()));
    }

    OpenApiSpec(OpenAPI openApi)
    {
        requireNonNull(openApi, "openApi is null");

        PathMetadataFactory pathMetadataFactory = new PathMetadataFactory(openApi);
        ImmutableList.Builder<Exception> exceptionsBuilder = ImmutableList.builder();
        ImmutableListMultimap.Builder<String, String> identifierToPathBuilder = ImmutableListMultimap.builder();
        ImmutableSet.Builder<ConnectorTableFunction> tableFunctionsBuilder = ImmutableSet.builder();
        ImmutableMap.Builder<String, OpenApiDecoder> pathToDecoderBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, OpenApiPaginationStrategy<?>> pathToPaginationStrategyBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, OpenApiAuthenticator> pathToAuthenticatorBuilder = ImmutableMap.builder();
        openApi.getPaths().keySet().forEach(path -> {
            String identifier = getIdentifier(path);
            if (identifier.isEmpty()) {
                log.warn("openApi specification uses empty path, ignoring");
                return; // Table functions require non-empty names.
            }
            identifierToPathBuilder.put(identifier, path);

            final Optional<PathMetadata> pathMetadata;
            try {
                pathMetadata = pathMetadataFactory.fromPath(path);
            }
            catch (Exception e) {
                exceptionsBuilder.add(e);
                return;
            }

            if (pathMetadata.isEmpty()) {
                return;
            }

            OpenApiDecoder decoder = pathMetadata.get().decoder();
            pathToDecoderBuilder.put(path, decoder);
            pathToPaginationStrategyBuilder.put(path, pathMetadata.get().paginationStrategy());
            pathToAuthenticatorBuilder.put(path, pathMetadata.get().authenticator());
            tableFunctionsBuilder.add(new OpenApiRequestTableFunction(
                    path,
                    identifier,
                    decoder.getColumnHandles()));
        });
        Map<String, Collection<String>> ambiguousIdentifiers = Maps.filterValues(
                identifierToPathBuilder.build().asMap(),
                paths -> paths.size() > 1);
        if (!ambiguousIdentifiers.isEmpty()) {
            ambiguousIdentifiers.forEach((identifier, paths) ->
                    exceptionsBuilder.add(new RuntimeException(
                            "Identifier %s maps to multiple API paths [%s]".formatted(
                                    identifier,
                                    join(",", paths)))));
        }
        List<Exception> exceptions = exceptionsBuilder.build();
        if (!exceptions.isEmpty()) {
            throw new TrinoException(
                    CONFIGURATION_INVALID,
                    new OpenApiValidationExceptions(exceptions));
        }
        this.tableFunctions = tableFunctionsBuilder.build();
        this.pathToDecoder = pathToDecoderBuilder.buildOrThrow();
        this.pathToPaginationStrategy = pathToPaginationStrategyBuilder.buildOrThrow();
        this.pathToAuthenticator = pathToAuthenticatorBuilder.buildOrThrow();
    }

    public static OpenAPI parse(String specLocation)
    {
        SwaggerParseResult result = new OpenAPIV3Parser().readLocation(specLocation, null, getParseOptions());
        if (result.isOpenapi31()) {
            // Contains changes to schema object behavior.
            // https://www.openapis.org/blog/2021/02/16/migrating-from-openapi-3-0-to-3-1-0
            // So to simplify we initially disable parsing 3.1.X specifications.
            throw new IllegalArgumentException("Connector supports OpenAPI specifications versions <= 3.0.X");
        }
        if (result.getMessages() != null && !result.getMessages().isEmpty()) {
            throw new IllegalArgumentException("Failed to parse the OpenAPI spec: " + join(", ", result.getMessages()));
        }
        return result.getOpenAPI();
    }

    private static class PathMetadataFactory
    {
        private final Map<String, ApiResponse> responses;
        private final Map<String, PathItem> paths;

        private PathMetadataFactory(OpenAPI openApi)
        {
            requireNonNull(openApi, "openApi is null");
            responses = Optional.ofNullable(openApi.getComponents())
                    .flatMap(components -> Optional.ofNullable(components.getResponses()))
                    .orElse(ImmutableMap.of());
            paths = openApi.getPaths();
        }

        private Optional<PathMetadata> fromPath(String path)
        {
            try {
                return fromPathItem(paths.get(path));
            }
            catch (Exception e) {
                throw new RuntimeException(
                        "Failed mapping path %s to table function (%s)".formatted(
                                path,
                                e.getMessage()),
                        e);
            }
        }

        private Optional<PathMetadata> fromPathItem(PathItem pathItem)
        {
            final Optional<Operation> getOperation;
            try {
                getOperation = getGetOperation(pathItem, paths);
            }
            catch (Exception e) {
                throw new TrinoException(
                        CONFIGURATION_INVALID,
                        "Failed getting GET operation (%s)".formatted(e.getMessage()), e);
            }
            return getOperation.flatMap(this::fromOperation);
        }

        private Optional<PathMetadata> fromOperation(Operation operation)
        {
            return getSuccessfulResponse(operation).flatMap(this::fromResponse);
        }

        private Optional<PathMetadata> fromResponse(ApiResponse response)
        {
            final Optional<Schema<?>> schema;
            try {
                schema = getJsonResponseSchema(response, responses);
            }
            catch (Exception e) {
                throw new TrinoException(
                        CONFIGURATION_INVALID,
                        "Failed mapping api response (%s)".formatted(e.getMessage()));
            }
            return schema.flatMap(this::fromResponseSchema);
        }

        private Optional<PathMetadata> fromResponseSchema(Schema<?> schema)
        {
            // TODO transform the schema with SchemaIr factory ...
            return Optional.of(new PathMetadata(
                    ONE_COLUMN_DECODER,
                    READ_ONCE_STRATEGY,
                    OpenApiAuthenticator.NONE));
        }
    }

    private record PathMetadata(
            OpenApiDecoder decoder,
            OpenApiPaginationStrategy<?> paginationStrategy,
            OpenApiAuthenticator authenticator)
    {
    }

    private static ParseOptions getParseOptions()
    {
        ParseOptions parseOptions = new ParseOptions();
        parseOptions.setResolveFully(false);
        parseOptions.setResolve(false);
        parseOptions.setInferSchemaType(false);
        return parseOptions;
    }

    public static String getIdentifier(String string)
    {
        return CaseFormat.LOWER_CAMEL.to(
                CaseFormat.LOWER_UNDERSCORE,
                string
                        .replaceAll("^/", "")
                        .replaceAll("[{}]", "")
                        .replace('/', '_')
                        .replace('-', '_'));
    }

    public OpenApiAuthenticator getAuthenticator(String path)
    {
        return pathToAuthenticator.get(path);
    }

    public OpenApiPaginationStrategy<?> getPaginationStrategy(String path)
    {
        return pathToPaginationStrategy.get(path);
    }

    public OpenApiDecoder getDecoder(String path)
    {
        return pathToDecoder.get(path);
    }

    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }
}
