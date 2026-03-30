/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;
import io.trino.spi.TrinoException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.collect.Iterables.concat;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNSUPPORTED_PARAMETER;
import static com.starburstdata.plugin.openapi.OpenApiSpec.HTTP_OK;
import static com.starburstdata.plugin.openapi.OpenApiSpec.MIME_JSON;
import static com.starburstdata.plugin.openapi.conversions.ReferenceUtil.extractRefKey;
import static java.util.Objects.requireNonNull;

public final class SpecUtil
{
    private SpecUtil()
    {
    }

    public static Optional<ApiResponse> getSuccessfulResponse(Operation operation)
    {
        // OK is one of the few success codes that returns content that will drive our output columns.
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status#successful_responses
        return Optional.ofNullable(operation.getResponses().get(HTTP_OK))
                // https://spec.openapis.org/oas/v3.0.4.html#patterned-fields-0
                // > "To define a range of response codes, this field MAY contain the uppercase wildcard character X"
                // > "Only the following range definitions are allowed: 1XX, 2XX, 3XX, 4XX, and 5XX"
                .or(() -> Optional.ofNullable(operation.getResponses().get("2XX")))
                // https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-13
                // 200 might not be explicitly defined, and may land in "default" field.
                .or(() -> Optional.ofNullable(operation.getResponses().get("default")));
    }

    /**
     * Merges parameters from the given {@code pathItem} and {@code operation}.
     * <p>
     * <blockquote>If a parameter is already defined in the Path Item,
     * the new definition will override it but can never remove it.</blockquote>
     * <a href="https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-7">- OAS 3.0.4</a>
     */
    public static Map<ParameterIdentifier, Parameter> getParameters(
            PathItem pathItem,
            Operation operation,
            Map<String, Parameter> referenceableParameters)
    {
        ImmutableMap.Builder<ParameterIdentifier, Parameter> builder = ImmutableMap.builder();
        for (Parameter parameterOrRef : concat(
                Optional.ofNullable(pathItem.getParameters()).orElse(ImmutableList.of()),
                Optional.ofNullable(operation.getParameters()).orElse(ImmutableList.of()))) {
            Parameter resolvedParameter = resolveParameter(parameterOrRef, referenceableParameters);
            builder.put(
                    new ParameterIdentifier(resolvedParameter.getName(), resolvedParameter.getIn()),
                    resolvedParameter);
        }
        return builder.buildKeepingLast();
    }

    /**
     * <blockquote>A unique parameter is defined by a combination of
     * a name and location.</blockquote>
     * <a href="https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-6">- OAS 3.0.4</a>
     */
    record ParameterIdentifier(String name, String in)
    {
    }

    public static Schema<?> getParameterSchema(Parameter parameter)
    {
        // https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-9
        // > Parameter Objects MUST include either a content field or a schema field, but not both
        // content keyword used for header or cookie parameter neither of which are supported right now.
        if (parameter.getContent() != null) {
            throw new TrinoException(OPENAPI_UNSUPPORTED_PARAMETER, "Parameter uses unsupported content keyword");
        }
        return requireNonNull(parameter.getSchema(), "Expected parameter without content keyword has schema keyword");
    }

    /**
     * @throws IllegalArgumentException If the response references form a cycle or an invalid key.
     */
    public static Optional<Schema<?>> getJsonResponseSchema(
            ApiResponse response,
            Map<String, ApiResponse> referenceableResponses)
    {
        return followReferencesUntil(
                responseOrRef -> responseOrRef.getContent() != null, "response",
                response,
                ApiResponse::get$ref,
                ImmutableList.of("components", "responses"),
                referenceableResponses)
                .flatMap(resolvedResponse ->
                        Optional.ofNullable(resolvedResponse.getContent().get(MIME_JSON)))
                .flatMap(mediaType -> Optional.ofNullable((Schema<?>) mediaType.getSchema()));
    }

    /**
     * @throws IllegalArgumentException If the path references form a cycle or an invalid key.
     */
    public static Optional<Operation> getGetOperation(
            PathItem pathItem,
            Map<String, PathItem> paths)
    {
        // https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-6
        // > "In case a Path Item Object field appears both in the defined object and the referenced object, the behavior is undefined"
        // Parser behavior is however to delete all other properties except "$ref".
        return followReferencesUntil(
                pathOrRef -> pathOrRef.getGet() != null, "path",
                pathItem,
                PathItem::get$ref,
                ImmutableList.of("paths"),
                paths).map(PathItem::getGet);
    }

    public static Parameter resolveParameter(
            Parameter parameter,
            Map<String, Parameter> parameters)
    {
        Optional<Parameter> resolvedParameter = followReferencesUntil(
                parameterOrRef -> parameterOrRef.getName() != null,
                "parameter",
                parameter,
                Parameter::get$ref,
                ImmutableList.of("components", "parameters"),
                parameters);
        if (resolvedParameter.isEmpty()) {
            // https://spec.openapis.org/oas/v3.0.4.html#parameter-object name is required.
            throw new IllegalArgumentException("Resolved parameter doesn't have name");
        }
        return resolvedParameter.get();
    }

    public static <T> Optional<T> followReferencesUntil(
            Predicate<T> condition,
            String context,
            T start,
            Function<T, String> extractReference,
            List<String> referencePrefix,
            Map<String, T> referenceableObjects)
    {
        Set<String> encounteredReferences = new HashSet<>();
        T current = start;
        while (!condition.test(current)) {
            String ref = extractReference.apply(current);
            if (ref == null) {
                return Optional.empty();
            }
            if (encounteredReferences.contains(ref)) {
                throw new IllegalArgumentException(
                        "Reference from %s forms a cycle".formatted(
                                context));
            }
            encounteredReferences.add(ref);
            String key = extractRefKey(referencePrefix, ref);
            if (!referenceableObjects.containsKey(key)) {
                throw new IllegalArgumentException("Reference refers to %s '%s' that doesn't exist".formatted(
                        context,
                        key));
            }
            current = referenceableObjects.get(key);
        }
        return Optional.of(current);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Schema<?>> castSchemaMap(Map<String, Schema> rawSchemaMap)
    {
        return (Map<String, Schema<?>>) (Map<String, ?>) rawSchemaMap;
    }
}
