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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.starburstdata.plugin.openapi.conversions.encoder.OpenApiParameterHandle;
import com.starburstdata.plugin.openapi.conversions.encoder.OpenApiParameterHandle.ParameterStyle;
import com.starburstdata.plugin.openapi.conversions.encoder.TypeEncoder.SerializedList;
import com.starburstdata.plugin.openapi.conversions.encoder.TypeEncoder.SerializedString;
import com.starburstdata.plugin.openapi.conversions.encoder.TypeEncoder.SerializedValue;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.ReturnTypeSpecification.DescribedTable;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;
import jakarta.ws.rs.core.UriBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.UrlEscapers.urlFormParameterEscaper;
import static com.starburstdata.plugin.openapi.OpenApiSpec.SCHEMA_NAME;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.StandardErrorCode.MISSING_ARGUMENT;
import static io.trino.spi.function.table.Descriptor.descriptor;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

class OpenApiRequestTableFunction
        extends AbstractConnectorTableFunction
{
    private final String path;
    private final UriBuilder pathTemplateBuilder;
    private final Map<String, OpenApiParameterHandle> identifierToParameterHandle;

    public OpenApiRequestTableFunction(
            URI baseUri,
            String path,
            String identifier,
            Map<String, OpenApiParameterHandle> identifierToParameterHandle,
            List<OpenApiColumnHandle> columns)
    {
        super(
                SCHEMA_NAME,
                identifier,
                identifierToParameterHandle.entrySet().stream()
                        .map(entry -> {
                            ScalarArgumentSpecification.Builder builder =
                                    ScalarArgumentSpecification.builder()
                                            .name(entry.getKey())
                                            .type(entry.getValue().getType());
                            if (!entry.getValue().required()) {
                                builder.defaultValue(null);
                            }
                            return builder.build();
                        })
                        .collect(toImmutableList()),
                new DescribedTable(descriptor(
                        columns.stream().map(OpenApiColumnHandle::name).collect(toImmutableList()),
                        columns.stream().map(OpenApiColumnHandle::type).collect(toImmutableList()))));
        this.path = requireNonNull(path, "path is null");
        this.identifierToParameterHandle = requireNonNull(
                identifierToParameterHandle,
                "identifierToParameterHandle is null");
        this.pathTemplateBuilder = UriBuilder.fromUri(
                stripTrailingSlash(baseUri.toString()).concat(path));
    }

    private static String stripTrailingSlash(String path)
    {
        if (path.endsWith("/")) {
            return path.substring(0, path.length() - 1);
        }
        return path;
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        checkArgument(
                identifierToParameterHandle.keySet().containsAll(arguments.keySet()),
                "Argument given that doesn't match known parameters");

        ImmutableMap.Builder<OpenApiParameterHandle, SerializedValue> parameterValueBuilder = ImmutableMap.builder();
        identifierToParameterHandle.forEach((identifier, parameterHandle) -> {
            Object scalarValue = Optional.ofNullable((ScalarArgument) arguments.get(identifier))
                    .map(ScalarArgument::getValue)
                    .orElse(null);
            if (parameterHandle.required() && scalarValue == null) {
                throw new TrinoException(
                        MISSING_ARGUMENT,
                        "Missing required parameter %s".formatted(identifier));
            }
            else if (scalarValue != null) {
                SerializedValue serializedValue = parameterHandle.typeEncoder().serialize(scalarValue);
                parameterValueBuilder.put(parameterHandle, serializedValue);
            }
        });

        URI uri = buildUri(parameterValueBuilder.buildOrThrow());

        return TableFunctionAnalysis.builder()
                .handle(new OpenApiTableFunctionHandle(new OpenApiRequestTableHandle(path, uri)))
                .build();
    }

    private URI buildUri(Map<OpenApiParameterHandle, SerializedValue> parameterValues)
    {
        ImmutableMap.Builder<String, String> pathParameterToValueBuilder = ImmutableMap.builder();
        ImmutableListMultimap.Builder<String, String> queryParameterValuesBuilder = ImmutableListMultimap.builder();
        parameterValues.forEach((parameterHandle, value) -> {
            String name = parameterHandle.name();
            switch (value) {
                case SerializedString(String string) -> {
                    switch (parameterHandle.parameterStyle()) {
                        case ParameterStyle.SIMPLE_PATH -> pathParameterToValueBuilder.put(name, string);
                        case ParameterStyle.FORM_EXPLODED_QUERY, ParameterStyle.FORM_UNEXPLODED_QUERY ->
                            queryParameterValuesBuilder.put(
                                    name,
                                    urlFormParameterEscaper().escape(string));
                    }
                }
                case SerializedList(List<String> values) -> {
                    switch (parameterHandle.parameterStyle()) {
                        case ParameterStyle.SIMPLE_PATH -> throw new UnsupportedOperationException(
                                "Unexpectedly serializing a non-string value to path parameter '%s'".formatted(name));
                        case ParameterStyle.FORM_EXPLODED_QUERY -> values.forEach(string ->
                                queryParameterValuesBuilder.put(name, urlFormParameterEscaper().escape(string)));
                        case ParameterStyle.FORM_UNEXPLODED_QUERY ->
                                queryParameterValuesBuilder.put(
                                        name,
                                        join(",", Lists.transform(values, urlFormParameterEscaper()::escape)));
                    }
                }
            }
        });
        URI resolvedTemplate = pathTemplateBuilder.buildFromMap(pathParameterToValueBuilder.buildOrThrow());
        return uriWithQueryParameters(resolvedTemplate, queryParameterValuesBuilder.build());
    }

    private static URI uriWithQueryParameters(URI uri, Multimap<String, String> queryParameters)
    {
        StringBuilder uriStringBuilder = new StringBuilder();
        Optional.ofNullable(uri.getScheme())
                .map(scheme -> scheme + ":")
                .ifPresent(uriStringBuilder::append);
        uriStringBuilder.append(uri.getRawSchemeSpecificPart());
        uriStringBuilder.append("?");
        Joiner.on("&")
                .withKeyValueSeparator("=")
                .appendTo(uriStringBuilder, queryParameters.entries());
        Optional.ofNullable(uri.getRawFragment()).ifPresent(uriStringBuilder::append);
        try {
            return new URI(uriStringBuilder.toString());
        }
        catch (URISyntaxException e) {
            throw new TrinoException(
                    FUNCTION_IMPLEMENTATION_ERROR,
                    "Failed to add query parameter arguments [%s] to existing URI '%s' (%s)".formatted(
                            Joiner.on(", ").withKeyValueSeparator(" = ").join(queryParameters.entries()),
                            uri,
                            e.getMessage()),
                    e);
        }
    }
}
