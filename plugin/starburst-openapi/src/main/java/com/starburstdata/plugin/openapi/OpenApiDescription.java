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

import com.fasterxml.jackson.core.JsonPointer;
import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy;
import com.starburstdata.plugin.openapi.SpecUtil.ParameterIdentifier;
import com.starburstdata.plugin.openapi.SpecUtil.Resolved;
import com.starburstdata.plugin.openapi.SpecUtil.SuccessfulResponse;
import com.starburstdata.plugin.openapi.authentication.OpenApiAuthenticator;
import com.starburstdata.plugin.openapi.conversions.SchemaIrFactory;
import com.starburstdata.plugin.openapi.conversions.decoder.OpenApiDecoder;
import com.starburstdata.plugin.openapi.conversions.decoder.OpenApiDecoderFactory;
import com.starburstdata.plugin.openapi.conversions.encoder.OpenApiParameterHandle;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;
import com.starburstdata.plugin.openapi.pagination.OpenApiPaginationStrategy;
import com.starburstdata.plugin.openapi.pagination.ReadOnceStrategy;
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
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.starburstdata.plugin.openapi.SpecUtil.getGetOperation;
import static com.starburstdata.plugin.openapi.SpecUtil.getJsonResponseSchema;
import static com.starburstdata.plugin.openapi.SpecUtil.getParameterSchema;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class OpenApiDescription
{
    private static final Logger log = Logger.get(OpenApiDescription.class);

    private static final ReadOnceStrategy READ_ONCE_STRATEGY = new ReadOnceStrategy();

    public static final String SCHEMA_NAME = "default";
    public static final String HTTP_OK = "200";
    public static final String MIME_JSON = "application/json";

    private final Set<ConnectorTableFunction> tableFunctions;
    private final Map<String, PathMetadata> pathMetadata;

    @Inject
    public OpenApiDescription(
            OpenApiConfig config,
            OpenApiAuthenticator authenticator,
            OpenApiDecoderFactory openApiDecoderFactory,
            OpenApiPaginationStrategy<?> paginationStrategy)
    {
        OpenAPI openApi = parse(config.getDescriptionLocation());
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
                CastPolicy.FALLBACK,
                openApiDecoderFactory,
                authenticator,
                paginationStrategy);
        this.tableFunctions = pathMetadata.entrySet().stream()
                .map(entry -> new OpenApiRequestTableFunction(
                        config.getBaseUri(),
                        entry.getKey(),
                        entry.getValue().identifier(),
                        entry.getValue().identifierToParameterHandle(),
                        entry.getValue().decoder().getColumnHandles()))
                .collect(toImmutableSet());
    }

    public static OpenAPI parse(String descriptionLocation)
    {
        SwaggerParseResult result = new OpenAPIV3Parser().readLocation(descriptionLocation, null, getParseOptions());
        if (result.isOpenapi31()) {
            // Contains changes to schema object behavior.
            // https://www.openapis.org/blog/2021/02/16/migrating-from-openapi-3-0-to-3-1-0
            // So to simplify we initially disable parsing 3.1.X descriptions.
            throw new IllegalArgumentException("Connector supports OpenAPI descriptions versions <= 3.0.X");
        }
        if (result.getMessages() != null && !result.getMessages().isEmpty()) {
            throw new IllegalArgumentException("Failed to parse the OpenAPI description: " + join(", ", result.getMessages()));
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
            OpenApiAuthenticator authenticator,
            OpenApiPaginationStrategy<?> paginationStrategy)
    {
        SchemaIrFactory schemaIrFactory = new SchemaIrFactory(castPolicy, schemas);
        ImmutableMap.Builder<String, PathMetadata> pathMetadataBuilder = ImmutableMap.builder();
        ImmutableList.Builder<Exception> exceptionsBuilder = ImmutableList.builder();
        Map<String, String> identifierToPath = new HashMap<>();
        paths.forEach((path, pathItem) -> {
            String identifier = getIdentifier(path);

            if (identifier.isEmpty()) {
                log.warn("openApi description uses empty path, ignoring");
                return; // Table functions require non-empty names.
            }
            String previousPath = identifierToPath.put(identifier, path);
            if (previousPath != null) {
                exceptionsBuilder.add(new SpecException(
                        "Identifier %s maps to multiple API paths [%s, %s]".formatted(
                                identifier,
                                previousPath,
                                path)));
            }

            List<String> pathPrefix = ImmutableList.of("paths", path);

            final Operation operation;
            final List<String> operationPrefix;
            try {
                Optional<Resolved<Operation>> operationOptional = getGetOperation(pathItem, paths);
                if (operationOptional.isEmpty()) {
                    return;
                }
                operation = operationOptional.get().value();
                operationPrefix = ImmutableList.<String>builder()
                        .addAll(pathPrefix)
                        .addAll(operationOptional.get().refPath())
                        .add("get")
                        .build();
            }
            catch (SpecException e) {
                exceptionsBuilder.add(e.fromPath(pathPrefix));
                return;
            }

            Optional<SuccessfulResponse> successfulResponse = SpecUtil.getSuccessfulResponse(operation);
            if (successfulResponse.isEmpty()) {
                return;
            }
            List<String> responsePrefix = ImmutableList.<String>builder()
                    .addAll(operationPrefix)
                    .add("responses")
                    .add(successfulResponse.get().code())
                    .build();

            final Schema<?> schema;
            final List<String> responseSchemaPrefix;
            try {
                Optional<Resolved<Schema<?>>> resolvedSchema = getJsonResponseSchema(successfulResponse.get().response(), responses);
                if (resolvedSchema.isEmpty()) {
                    return;
                }
                schema = resolvedSchema.get().value();
                responseSchemaPrefix = ImmutableList.<String>builder()
                        .addAll(responsePrefix)
                        .addAll(resolvedSchema.get().refPath())
                        .add("content")
                        .add(MIME_JSON)
                        .add("schema")
                        .build();
            }
            catch (SpecException e) {
                exceptionsBuilder.add(e.fromPath(responsePrefix));
                return;
            }

            final OpenApiDecoder decoder;
            try {
                SchemaIr schemaIr = schemaIrFactory.convert(schema);
                decoder = openApiDecoderFactory.createFrom(schemaIr);
            }
            catch (SpecException e) {
                exceptionsBuilder.add(e.fromPath(responseSchemaPrefix));
                return;
            }

            List<Parameter> rawParameters = ImmutableList.<Parameter>builder()
                    .addAll(Optional.ofNullable(pathItem.getParameters()).orElse(ImmutableList.of()))
                    .addAll(Optional.ofNullable(operation.getParameters()).orElse(ImmutableList.of()))
                    .build();
            // Operation parameters override Path Item parameters sharing the same (name, in).
            // https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-7
            LinkedHashMap<ParameterIdentifier, IndexedParameter> resolvedParametersByIdentifier = new LinkedHashMap<>();
            boolean parameterFailed = false;
            for (int i = 0; i < rawParameters.size(); i++) {
                List<String> parameterPrefix = parameterPrefix(operationPrefix, i);
                try {
                    Resolved<Parameter> resolvedParameter = SpecUtil.resolveParameter(rawParameters.get(i), parameters);
                    resolvedParametersByIdentifier.put(
                            new ParameterIdentifier(
                                    resolvedParameter.value().getName(),
                                    resolvedParameter.value().getIn()),
                            new IndexedParameter(i, resolvedParameter.value(), resolvedParameter.refPath()));
                }
                catch (SpecException e) {
                    exceptionsBuilder.add(e.fromPath(parameterPrefix));
                    parameterFailed = true;
                }
            }

            Set<String> argumentNames = new HashSet<>();
            ImmutableMap.Builder<String, OpenApiParameterHandle> identifierToParameterHandleBuilder =
                    ImmutableMap.builder();
            for (IndexedParameter indexed : resolvedParametersByIdentifier.values()) {
                int i = indexed.index();
                Parameter resolvedParameter = indexed.parameter();
                List<String> parameterPrefix = ImmutableList.<String>builder()
                        .addAll(parameterPrefix(operationPrefix, i))
                        .addAll(indexed.refPath())
                        .build();
                String argumentName = getIdentifier(resolvedParameter.getName()).toUpperCase(ENGLISH);
                if (!argumentNames.add(argumentName)) {
                    exceptionsBuilder.add(new SpecException(
                            "Cannot refer to parameter '%s' unambiguously, parameter with identifier '%s' already exists".formatted(
                                    resolvedParameter.getName(),
                                    argumentName))
                            .fromPath(ImmutableList.<String>builder()
                                    .addAll(parameterPrefix)
                                    .add("name")
                                    .build()));
                    parameterFailed = true;
                    continue;
                }
                final Schema<?> parameterSchema;
                try {
                    parameterSchema = getParameterSchema(resolvedParameter);
                }
                catch (SpecException e) {
                    exceptionsBuilder.add(e.fromPath(parameterPrefix));
                    parameterFailed = true;
                    continue;
                }
                final SchemaIr parameterSchemaIr;
                try {
                    parameterSchemaIr = schemaIrFactory.convert(parameterSchema);
                }
                catch (SpecException e) {
                    exceptionsBuilder.add(e.fromPath(ImmutableList.<String>builder()
                            .addAll(parameterPrefix)
                            .add("schema")
                            .build()));
                    parameterFailed = true;
                    continue;
                }
                try {
                    identifierToParameterHandleBuilder.put(
                            argumentName,
                            OpenApiParameterHandle.from(resolvedParameter, parameterSchemaIr));
                }
                catch (TrinoException e) {
                    exceptionsBuilder.add(new SpecException(e.getMessage())
                            .withCause(e)
                            .fromPath(parameterPrefix));
                    parameterFailed = true;
                }
            }
            if (parameterFailed) {
                return;
            }
            Optional<String> description = Optional.ofNullable(operation.getSummary())
                    .filter(summary -> !summary.isEmpty())
                    .or(() -> Optional.ofNullable(operation.getDescription()));
            pathMetadataBuilder.put(
                    path,
                    new PathMetadata(
                            identifier,
                            description,
                            decoder,
                            identifierToParameterHandleBuilder.buildOrThrow(),
                            paginationStrategy,
                            authenticator));
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
            Optional<String> description,
            OpenApiDecoder decoder,
            Map<String, OpenApiParameterHandle> identifierToParameterHandle,
            OpenApiPaginationStrategy<?> paginationStrategy,
            OpenApiAuthenticator authenticator) {}

    private record IndexedParameter(int index, Parameter parameter, List<String> refPath) {}

    private static List<String> parameterPrefix(List<String> operationPrefix, int index)
    {
        return ImmutableList.<String>builder()
                .addAll(operationPrefix)
                .add("parameters[%d]".formatted(index))
                .build();
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

    public OpenApiPaginationStrategy<?> getPaginationStrategy(OpenApiRequestTableHandle requestTableHandle)
    {
        String path = requestTableHandle.path();
        OpenApiPaginationStrategy<?> paginationStrategy = pathMetadata.get(path).paginationStrategy();

        List<OpenApiColumnHandle> columnHandles = pathMetadata.get(path).decoder().getColumnHandles();
        List<String> providedParameters = requestTableHandle.providedParameters();
        Set<String> paginationStrategyParameterNames = paginationStrategy.getParameterNames();
        if (new HashSet<>(requestTableHandle.parameterNames()).containsAll(paginationStrategyParameterNames)
                && Collections.disjoint(paginationStrategyParameterNames, providedParameters)
                && responseContainsRequiredResponseFieldJsonPointers(columnHandles, paginationStrategy.requiredResponseColumnsPaths())) {
            // return the configured pagination strategy only if none of the pagination strategy
            // parameters are provided by the user and all required response column paths are present
            return paginationStrategy;
        }

        return READ_ONCE_STRATEGY;
    }

    private boolean responseContainsRequiredResponseFieldJsonPointers(List<OpenApiColumnHandle> columnHandles, Set<JsonPointer> requiredResponseFieldJsonPointers)
    {
        if (requiredResponseFieldJsonPointers.isEmpty()) {
            return true;
        }

        for (JsonPointer pointer : requiredResponseFieldJsonPointers) {
            // "/paging/next" -> ["", "paging", "next"]; skip segments[0] (empty, before leading '/')
            String[] segments = pointer.toString().split("/", -1);
            if (segments.length <= 1) {
                continue; // empty pointer refers to root — always present
            }
            String topLevel = segments[1];
            Optional<OpenApiColumnHandle> column = columnHandles.stream()
                    .filter(handle -> handle.name().equals(topLevel))
                    .findFirst();
            if (column.isEmpty()) {
                return false;
            }
            Type columnType = column.get().type();
            for (int i = 2; i < segments.length; i++) {
                if (!(columnType instanceof RowType rowType)) {
                    return false;
                }
                String segment = segments[i];
                Optional<RowType.Field> field = rowType.getFields().stream()
                        .filter(f -> f.getName().filter(segment::equals).isPresent())
                        .findFirst();
                if (field.isEmpty()) {
                    return false;
                }
                columnType = field.get().getType();
            }
        }

        return true;
    }

    public OpenApiDecoder getDecoder(String path)
    {
        return pathMetadata.get(path).decoder();
    }

    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return tableFunctions;
    }

    public record TableFunctionDetail(
            String functionName,
            String apiPath,
            Optional<String> description,
            Map<String, OpenApiParameterHandle> inputParameters,
            List<OpenApiColumnHandle> outputColumns) {}

    public List<TableFunctionDetail> getTableFunctionDetails()
    {
        return pathMetadata.entrySet().stream()
                .map(entry -> new TableFunctionDetail(
                        entry.getValue().identifier(),
                        entry.getKey(),
                        entry.getValue().description(),
                        entry.getValue().identifierToParameterHandle(),
                        entry.getValue().decoder().getColumnHandles()))
                .collect(toImmutableList());
    }
}
