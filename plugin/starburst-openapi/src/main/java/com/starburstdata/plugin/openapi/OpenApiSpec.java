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
import com.starburstdata.plugin.openapi.SpecUtil.ParameterIdentifier;
import com.starburstdata.plugin.openapi.authentication.OpenApiAuthenticator;
import com.starburstdata.plugin.openapi.conversions.SchemaIrFactory;
import com.starburstdata.plugin.openapi.conversions.SchemaIrFactory.CastPolicy;
import com.starburstdata.plugin.openapi.conversions.decoder.OpenApiDecoder;
import com.starburstdata.plugin.openapi.conversions.decoder.OpenApiDecoderFactory;
import com.starburstdata.plugin.openapi.conversions.encoder.OpenApiParameterHandle;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;
import com.starburstdata.plugin.openapi.pagination.OpenApiPaginationStrategy;
import io.airlift.log.Logger;
import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.swagger.v3.parser.OpenAPIV3Parser;
import io.swagger.v3.parser.core.models.ParseOptions;
import io.swagger.v3.parser.core.models.SwaggerParseResult;
import io.trino.spi.TrinoException;
import io.trino.spi.function.table.ConnectorTableFunction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_AMBIGUOUS_REFERENCE;
import static com.starburstdata.plugin.openapi.SpecUtil.getGetOperation;
import static com.starburstdata.plugin.openapi.SpecUtil.getJsonResponseSchema;
import static com.starburstdata.plugin.openapi.SpecUtil.getParameterSchema;
import static com.starburstdata.plugin.openapi.SpecUtil.getParameters;
import static com.starburstdata.plugin.openapi.pagination.OpenApiPaginationStrategy.READ_ONCE_STRATEGY;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
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
    public OpenApiSpec(
            OpenApiConfig config,
            OpenApiAuthenticator authenticator,
            OpenApiDecoderFactory openApiDecoderFactory)
    {
        OpenAPI openApi = parse(config.getSpecLocation());
        requireNonNull(openApi, "openApi is null");

        Optional<Components> componentsOptional = Optional.ofNullable(openApi.getComponents());
        Map<String, ApiResponse> referenceableResponses = componentsOptional
                .flatMap(components -> Optional.ofNullable(components.getResponses()))
                .orElse(ImmutableMap.of());
        Map<String, Schema<?>> referenceableSchemas = componentsOptional
                .flatMap(components -> Optional.ofNullable(components.getSchemas()))
                .map(SpecUtil::castSchemaMap)
                .orElse(ImmutableMap.of());
        Map<String, Parameter> referenceableParameters = componentsOptional
                .flatMap(components -> Optional.ofNullable(components.getParameters()))
                .orElse(ImmutableMap.of());

        this.pathMetadata = getPathMetadata(
                openApi.getPaths(),
                referenceableResponses,
                referenceableSchemas,
                referenceableParameters,
                CastPolicy.JSON,
                openApiDecoderFactory,
                authenticator);
        this.tableFunctions = pathMetadata.entrySet().stream()
                .map(entry -> new OpenApiRequestTableFunction(
                        config.getBaseUri(),
                        entry.getKey(),
                        entry.getValue().identifier(),
                        entry.getValue().identifierToParameterHandle(),
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

    private static Map<String, PathMetadata> getPathMetadata(
            Map<String, PathItem> paths,
            Map<String, ApiResponse> responses,
            Map<String, Schema<?>> schemas,
            Map<String, Parameter> parameters,
            CastPolicy castPolicy,
            OpenApiDecoderFactory openApiDecoderFactory,
            OpenApiAuthenticator authenticator)
    {
        SchemaIrFactory schemaIrFactory = new SchemaIrFactory(castPolicy, schemas);
        ImmutableMap.Builder<String, PathMetadata> pathMetadataBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Exception> exceptionsBuilder = ImmutableList.builder();
        Map<String, String> identifierToPath = new HashMap<>();
        paths.forEach((path, pathItem) -> {
            String identifier = getIdentifier(path);

            if (identifier.isEmpty()) {
                log.warn("openApi specification uses empty path, ignoring");
                return; // Table functions require non-empty names.
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
                Optional<Operation> operationOptional = getGetOperation(pathItem, paths);
                if (operationOptional.isEmpty()) {
                    return;
                }
                Operation operation = operationOptional.get();
                Optional<ApiResponse> response = operationOptional.flatMap(SpecUtil::getSuccessfulResponse);
                Optional<Schema<?>> schema = response.flatMap(r -> getJsonResponseSchema(r, responses));
                if (schema.isEmpty()) {
                    return;
                }
                SchemaIr schemaIr = schemaIrFactory.convert(schema.get());
                OpenApiDecoder decoder = openApiDecoderFactory.createFrom(schemaIr);

                Map<ParameterIdentifier, Parameter> resolvedParameters = getParameters(
                        pathItem,
                        operation,
                        parameters);
                Set<String> argumentNames = new HashSet<>();
                ImmutableMap.Builder<String, OpenApiParameterHandle> identifierToParameterHandleBuilder =
                        ImmutableMap.builder();
                for (ParameterIdentifier parameterIdentifier : resolvedParameters.keySet()) {
                    String argumentName = getIdentifier(parameterIdentifier.name()).toUpperCase(ENGLISH);
                    Parameter resolvedParameter = resolvedParameters.get(parameterIdentifier);
                    if (!argumentNames.add(argumentName)) {
                        throw new TrinoException(
                                OPENAPI_AMBIGUOUS_REFERENCE,
                                "Cannot refer to parameter '%s' unambiguously, parameter with identifier '%s' already exists".formatted(
                                        parameterIdentifier.name(),
                                        argumentName));
                    }
                    Schema<?> parameterSchema = getParameterSchema(resolvedParameter);
                    SchemaIr parameterSchemaIr = schemaIrFactory.convert(parameterSchema);
                    identifierToParameterHandleBuilder.put(
                            argumentName,
                            OpenApiParameterHandle.from(resolvedParameter, parameterSchemaIr));
                }
                pathMetadataBuilder.put(
                        path,
                        new PathMetadata(
                                identifier,
                                decoder,
                                identifierToParameterHandleBuilder.buildOrThrow(),
                                READ_ONCE_STRATEGY,
                                authenticator));
            }
            catch (Exception e) {
                exceptionsBuilder.add(new RuntimeException(
                        "Failed to transform path %s (%s)".formatted(path, e.getMessage()),
                        e));
            }
        });

        List<Exception> exceptions = exceptionsBuilder.build();
        if (!exceptions.isEmpty()) {
            throw new TrinoException(
                    CONFIGURATION_INVALID,
                    new OpenApiValidationExceptions(exceptions));
        }

        return pathMetadataBuilder.buildOrThrow();
    }

    private record PathMetadata(
            String identifier,
            OpenApiDecoder decoder,
            Map<String, OpenApiParameterHandle> identifierToParameterHandle,
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
