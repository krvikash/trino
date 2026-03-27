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
import com.google.common.collect.ImmutableMap;
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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.plugin.openapi.SpecUtil.getGetOperation;
import static com.starburstdata.plugin.openapi.SpecUtil.getJsonResponseSchema;
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
    private final Map<String, PathMetadata> pathMetadata;

    @Inject
    public OpenApiSpec(OpenApiConfig config)
    {
        this(parse(config.getSpecLocation()));
    }

    OpenApiSpec(OpenAPI openApi)
    {
        requireNonNull(openApi, "openApi is null");

        Map<String, ApiResponse> referenceableResponses = Optional.ofNullable(openApi.getComponents())
                .flatMap(components -> Optional.ofNullable(components.getResponses()))
                .orElse(ImmutableMap.of());

        this.pathMetadata = getPathMetadata(openApi.getPaths(), referenceableResponses);
        this.tableFunctions = pathMetadata.entrySet().stream()
                .map(entry -> new OpenApiRequestTableFunction(
                        entry.getKey(),
                        entry.getValue().identifier(),
                        entry.getValue().decoder().getColumnHandles()))
                .collect(toImmutableSet());
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

    private static Map<String, PathMetadata> getPathMetadata(Map<String, PathItem> paths, Map<String, ApiResponse> responses)
    {
        ImmutableMap.Builder<String, PathMetadata> pathMetadataBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Exception> exceptionsBuilder = ImmutableList.builder();
        Map<String, String> identifierToPath = new HashMap<>();
        for (Map.Entry<String, PathItem> entry : paths.entrySet()) {
            String path = entry.getKey();
            String identifier = getIdentifier(path);
            PathItem pathItem = entry.getValue();

            if (identifier.isEmpty()) {
                log.warn("openApi specification uses empty path, ignoring");
                continue; // Table functions require non-empty names.
            }
            String previousPath = identifierToPath.put(identifier, path);
            if (previousPath != null) {
                exceptionsBuilder.add(new RuntimeException(
                        "Identifier %s maps to multiple API paths [%s, %s]".formatted(
                                identifier,
                                previousPath,
                                path)));
            }

            try {
                Optional<Operation> operation = getGetOperation(pathItem, paths);
                Optional<ApiResponse> response = operation.flatMap(SpecUtil::getSuccessfulResponse);
                Optional<Schema<?>> schema = response.flatMap(r -> getJsonResponseSchema(r, responses));

                // TODO transform the schema with SchemaIr factory ...
                schema.map(_ -> new PathMetadata(path,
                        identifier,
                        ONE_COLUMN_DECODER,
                        READ_ONCE_STRATEGY,
                        OpenApiAuthenticator.NONE)).ifPresent(pm -> pathMetadataBuilder.put(path, pm));
            }
            catch (Exception e) {
                exceptionsBuilder.add(new RuntimeException("...", e));
            }
        }

        List<Exception> exceptions = exceptionsBuilder.build();
        if (!exceptions.isEmpty()) {
            throw new TrinoException(
                    CONFIGURATION_INVALID,
                    new OpenApiValidationExceptions(exceptions));
        }

        return pathMetadataBuilder.buildOrThrow();
    }

    private record PathMetadata(
            String path,
            String identifier,
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
        return pathMetadata.get(path).authenticator();
    }

    public OpenApiPaginationStrategy<?> getPaginationStrategy(String path)
    {
        return pathMetadata.get(path).paginationStrategy();
    }

    public OpenApiDecoder getDecoder(String path)
    {
        return pathMetadata.get(path).decoder();
    }

    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }
}
