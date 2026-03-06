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
import com.google.inject.Inject;
import com.starburstdata.plugin.openapi.OpenApiValidationExceptions.AmbiguousTableFunctionPath;
import com.starburstdata.plugin.openapi.OpenApiValidationExceptions.BadPathItem;
import com.starburstdata.plugin.openapi.OpenApiValidationExceptions.BadResponseReference;
import com.starburstdata.plugin.openapi.OpenApiValidationExceptions.FailedValidation;
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
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.plugin.openapi.SpecUtil.getGetOperation;
import static com.starburstdata.plugin.openapi.SpecUtil.getJsonResponseSchema;
import static com.starburstdata.plugin.openapi.SpecUtil.getSuccessfulResponse;
import static com.starburstdata.plugin.openapi.conversions.OpenApiDecoder.ONE_COLUMN_DECODER;
import static com.starburstdata.plugin.openapi.pagination.OpenApiPaginationStrategy.READ_ONCE_STRATEGY;
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

        Map<String, ApiResponse> referenceableResponses = Optional.ofNullable(openApi.getComponents())
                .flatMap(components -> Optional.ofNullable(components.getResponses()))
                .orElse(ImmutableMap.of());
        ImmutableList.Builder<FailedValidation> failedValidationsBuilder = ImmutableList.builder();
        ImmutableListMultimap.Builder<String, OpenApiRequestTableFunction> identifierToTableFunctionsBuilder =
                ImmutableListMultimap.builder();
        ImmutableMap.Builder<String, OpenApiDecoder> pathToDecoderBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, OpenApiPaginationStrategy<?>> pathToPaginationStrategyBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<String, OpenApiAuthenticator> pathToAuthenticatorBuilder = ImmutableMap.builder();
        openApi.getPaths().forEach((String path, PathItem pathItem) -> {
            String identifier = getIdentifier(path);
            if (identifier.isEmpty()) {
                log.warn("openApi specification uses empty path, ignoring");
                return; // Table functions require non-empty names.
            }
            final Optional<Operation> getOperation;
            try {
                getOperation = getGetOperation(pathItem, openApi.getPaths());
            }
            catch (IllegalArgumentException e) {
                failedValidationsBuilder.add(new BadPathItem(path, e.getMessage()));
                return;
            }
            if (getOperation.isEmpty()) {
                return; // ENG-7359, we choose to only handle requests we know are read-only for now.
            }
            Optional<ApiResponse> apiResponse = getSuccessfulResponse(getOperation.get());
            if (apiResponse.isEmpty()) {
                return;
            }
            final Optional<Schema<?>> okJsonResponseSchema;
            try {
                okJsonResponseSchema = getJsonResponseSchema(apiResponse.get(), referenceableResponses);
            }
            catch (IllegalArgumentException e) {
                failedValidationsBuilder.add(new BadResponseReference(path, e.getMessage()));
                return;
            }
            if (okJsonResponseSchema.isEmpty()) {
                return;
            }
            OpenApiDecoder decoder = ONE_COLUMN_DECODER;
            pathToDecoderBuilder.put(path, decoder);
            pathToPaginationStrategyBuilder.put(path, READ_ONCE_STRATEGY);
            pathToAuthenticatorBuilder.put(path, OpenApiAuthenticator.NONE);
            identifierToTableFunctionsBuilder.put(identifier, new OpenApiRequestTableFunction(path, identifier, decoder.getColumnHandles()));
        });
        Map<String, Collection<OpenApiRequestTableFunction>> identifierToTableFunctions =
                identifierToTableFunctionsBuilder.build().asMap();
        identifierToTableFunctions.entrySet()
                .stream()
                .filter(entry -> entry.getValue().size() > 1)
                .map(entry -> new AmbiguousTableFunctionPath(
                        entry.getKey(),
                        entry.getValue()
                                .stream()
                                .map(OpenApiRequestTableFunction::getPath)
                                .collect(toImmutableList())))
                .forEach(failedValidationsBuilder::add);
        List<FailedValidation> failedValidations = failedValidationsBuilder.build();
        if (!failedValidations.isEmpty()) {
            throw new TrinoException(
                    StandardErrorCode.CONFIGURATION_INVALID,
                    new OpenApiValidationExceptions(failedValidations));
        }
        this.tableFunctions = identifierToTableFunctions
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(toImmutableSet());
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
            throw new IllegalArgumentException("Failed to parse the OpenAPI spec: " + String.join(", ", result.getMessages()));
        }
        return result.getOpenAPI();
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
