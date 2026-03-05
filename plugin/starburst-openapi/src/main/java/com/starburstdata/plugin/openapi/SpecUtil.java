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
import com.starburstdata.plugin.openapi.conversions.ReferenceUtil;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.responses.ApiResponse;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starburstdata.plugin.openapi.OpenApiSpec.HTTP_OK;
import static com.starburstdata.plugin.openapi.OpenApiSpec.MIME_JSON;

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
     * @throws IllegalArgumentException If the response references form a cycle or an invalid key.
     */
    public static Optional<Schema<?>> getJsonResponseSchema(
            ApiResponse response,
            Map<String, ApiResponse> referenceableResponses)
    {
        Set<String> encounteredReferences = new HashSet<>();
        ApiResponse mutableResponse = response;
        final Content finalContent;
        while (true) {
            Content nullableContent = mutableResponse.getContent();
            if (nullableContent != null) {
                finalContent = nullableContent;
                break;
            }
            String ref = mutableResponse.get$ref();
            if (ref == null) {
                return Optional.empty();
            }
            if (encounteredReferences.contains(ref)) {
                throw new IllegalArgumentException("Response references form a cycle");
            }
            encounteredReferences.add(ref);
            String key = ReferenceUtil.extractRefKey(ImmutableList.of("components", "responses"), ref);
            if (!referenceableResponses.containsKey(key)) {
                throw new IllegalArgumentException(
                        "Response references re-usable response that doesn't exist: %s".formatted(key));
            }
            mutableResponse = referenceableResponses.get(key);
        }
        return Optional.ofNullable(finalContent.get(MIME_JSON))
                .flatMap(mediaType -> Optional.<Schema<?>>ofNullable(mediaType.getSchema()));
    }

    /**
     * @throws IllegalArgumentException If the path references form a cycle or an invalid key.
     */
    public static Optional<Operation> getGetOperation(
            PathItem pathItem,
            Map<String, PathItem> paths)
    {
        Set<String> encounteredReferences = new HashSet<>();
        PathItem mutablePathItem = pathItem;
        final Operation finalOperation;
        while (true) {
            // https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-6
            // > "In case a Path Item Object field appears both in the defined object and the referenced object, the behavior is undefined"
            // Parser behavior is however to delete all other properties except "$ref".
            Operation operation = mutablePathItem.getGet();
            if (operation != null) {
                finalOperation = operation;
                break;
            }
            String ref = mutablePathItem.get$ref();
            if (ref == null) {
                return Optional.empty();
            }
            if (encounteredReferences.contains(ref)) {
                throw new IllegalArgumentException("Path references form a cycle");
            }
            encounteredReferences.add(ref);
            String key = ReferenceUtil.extractRefKey(ImmutableList.of("paths"), ref);
            if (!paths.containsKey(key)) {
                throw new IllegalArgumentException(
                        "Path references path that doesn't exist: %s".formatted(key));
            }
            mutablePathItem = paths.get(key);
        }
        return Optional.of(finalOperation);
    }
}
