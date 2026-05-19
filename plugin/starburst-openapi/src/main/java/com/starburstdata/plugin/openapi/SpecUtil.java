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
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.responses.ApiResponse;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.starburstdata.plugin.openapi.OpenApiSpec.HTTP_OK;
import static com.starburstdata.plugin.openapi.OpenApiSpec.MIME_JSON;
import static com.starburstdata.plugin.openapi.conversions.ReferenceUtil.extractRefKey;
import static java.util.Objects.requireNonNull;

public final class SpecUtil
{
    private SpecUtil() {}

    public static Optional<SuccessfulResponse> getSuccessfulResponse(Operation operation)
    {
        // OK is one of the few success codes that returns content that will drive our output columns.
        // https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status#successful_responses
        // https://spec.openapis.org/oas/v3.0.4.html#patterned-fields-0
        // > "To define a range of response codes, this field MAY contain the uppercase wildcard character X"
        // > "Only the following range definitions are allowed: 1XX, 2XX, 3XX, 4XX, and 5XX"
        // https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-13
        // 200 might not be explicitly defined, and may land in "default" field.
        for (String code : ImmutableList.of(HTTP_OK, "2XX", "default")) {
            ApiResponse response = operation.getResponses().get(code);
            if (response != null) {
                return Optional.of(new SuccessfulResponse(code, response));
            }
        }
        return Optional.empty();
    }

    public record SuccessfulResponse(String code, ApiResponse response) {}

    /**
     * A value produced by resolving a chain of {@code $ref}s, along with the
     * document-path segments that locate the resolved value relative to where
     * resolution began. The {@code refPath} alternates {@code "$ref"} segments
     * with the resolved reference key, e.g. {@code ["$ref", "Pet", "$ref", "BasePet"]}.
     */
    public record Resolved<T>(T value, List<String> refPath)
    {
        public Resolved
        {
            requireNonNull(value, "value is null");
            refPath = ImmutableList.copyOf(requireNonNull(refPath, "refPath is null"));
        }

        public <R> Resolved<R> withValue(R newValue)
        {
            return new Resolved<>(newValue, refPath);
        }
    }

    /**
     * <blockquote>A unique parameter is defined by a combination of
     * a name and location.</blockquote>
     * <a href="https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-6">- OAS 3.0.4</a>
     */
    record ParameterIdentifier(String name, String in) {}

    public static Schema<?> getParameterSchema(Parameter parameter)
            throws SpecException
    {
        // https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-9
        // > Parameter Objects MUST include either a content field or a schema field, but not both
        // content keyword used for header or cookie parameter neither of which are supported right now.
        if (parameter.getContent() != null) {
            throw new SpecException("Parameter uses unsupported content keyword");
        }
        return requireNonNull(parameter.getSchema(), "Expected parameter without content keyword has schema keyword");
    }

    public static Optional<Resolved<Schema<?>>> getJsonResponseSchema(
            ApiResponse response,
            Map<String, ApiResponse> referenceableResponses)
            throws SpecException
    {
        Optional<Resolved<ApiResponse>> resolvedResponse = followReferencesUntil(
                responseOrRef -> responseOrRef.getContent() != null,
                "response",
                response,
                ApiResponse::get$ref,
                ImmutableList.of("components", "responses"),
                referenceableResponses);
        if (resolvedResponse.isEmpty()) {
            return Optional.empty();
        }
        ApiResponse resolved = resolvedResponse.get().value();
        return Optional.ofNullable(resolved.getContent().get(MIME_JSON))
                .map(mediaType -> (Schema<?>) mediaType.getSchema())
                .map(schema -> resolvedResponse.get().withValue(schema));
    }

    public static Optional<Resolved<Operation>> getGetOperation(
            PathItem pathItem,
            Map<String, PathItem> paths)
            throws SpecException
    {
        // https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-6
        // > "In case a Path Item Object field appears both in the defined object and the referenced object, the behavior is undefined"
        // Parser behavior is however to delete all other properties except "$ref".
        return followReferencesUntil(
                pathOrRef -> pathOrRef.getGet() != null,
                "path",
                pathItem,
                PathItem::get$ref,
                ImmutableList.of("paths"),
                paths).map(resolved -> resolved.withValue(resolved.value().getGet()));
    }

    public static Resolved<Parameter> resolveParameter(
            Parameter parameter,
            Map<String, Parameter> parameters)
            throws SpecException
    {
        Optional<Resolved<Parameter>> resolvedParameter = followReferencesUntil(
                parameterOrRef -> parameterOrRef.getName() != null,
                "parameter",
                parameter,
                Parameter::get$ref,
                ImmutableList.of("components", "parameters"),
                parameters);
        if (resolvedParameter.isEmpty()) {
            // https://spec.openapis.org/oas/v3.0.4.html#parameter-object name is required.
            throw new SpecException("Resolved parameter doesn't have name");
        }
        return resolvedParameter.get();
    }

    public static <T> Optional<Resolved<T>> followReferencesUntil(
            Predicate<T> condition,
            String context,
            T start,
            Function<T, String> extractReference,
            List<String> referencePrefix,
            Map<String, T> referenceableObjects)
            throws SpecException
    {
        Set<String> encounteredReferences = new HashSet<>();
        T current = start;
        List<String> refPath = new ArrayList<>();
        while (!condition.test(current)) {
            String ref = extractReference.apply(current);
            if (ref == null) {
                return Optional.empty();
            }
            if (encounteredReferences.contains(ref)) {
                throw new SpecException("Reference from %s forms a cycle".formatted(context))
                        .fromPath(atRef(refPath));
            }
            encounteredReferences.add(ref);
            final String key;
            try {
                key = extractRefKey(referencePrefix, ref);
            }
            catch (IllegalArgumentException e) {
                throw new SpecException(e.getMessage()).withCause(e).fromPath(atRef(refPath));
            }
            if (!referenceableObjects.containsKey(key)) {
                throw new SpecException("Reference refers to %s '%s' that doesn't exist".formatted(
                        context,
                        key))
                        .fromPath(atRef(refPath));
            }
            current = referenceableObjects.get(key);
            refPath.add("$ref");
            refPath.add(key);
        }
        return Optional.of(new Resolved<>(current, refPath));
    }

    private static List<String> atRef(List<String> refPath)
    {
        return ImmutableList.<String>builderWithExpectedSize(refPath.size() + 1)
                .addAll(refPath)
                .add("$ref")
                .build();
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Schema<?>> castSchemaMap(Map<String, Schema> rawSchemaMap)
    {
        return (Map<String, Schema<?>>) (Map<String, ?>) rawSchemaMap;
    }
}
