/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.plugin.openapi.conversions.encoder;

import com.starburstdata.plugin.openapi.OpenApiConfig.CastPolicy;
import com.starburstdata.plugin.openapi.conversions.ir.SchemaIr;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.parameters.Parameter.StyleEnum;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.starburstdata.plugin.openapi.OpenApiErrorCode.OPENAPI_UNSUPPORTED_PARAMETER;
import static com.starburstdata.plugin.openapi.conversions.encoder.OpenApiParameterHandle.ParameterStyle.FORM_EXPLODED_QUERY;
import static com.starburstdata.plugin.openapi.conversions.encoder.OpenApiParameterHandle.ParameterStyle.FORM_UNEXPLODED_QUERY;
import static com.starburstdata.plugin.openapi.conversions.encoder.OpenApiParameterHandle.ParameterStyle.SIMPLE_PATH;
import static io.swagger.v3.oas.models.parameters.Parameter.StyleEnum.FORM;
import static io.swagger.v3.oas.models.parameters.Parameter.StyleEnum.SIMPLE;

public record OpenApiParameterHandle(
        String name,
        ParameterStyle parameterStyle,
        TypeEncoder typeEncoder,
        boolean required)
{
    public Type getType()
    {
        return typeEncoder.getType();
    }

    /**
     * Represents a unique combination of parameter
     * <a href="https://spec.openapis.org/oas/v3.0.4.html#parameter-locations">location</a>,
     * <a href="https://spec.openapis.org/oas/v3.0.4.html#style-values">style</a>,
     * and <a href="https://spec.openapis.org/oas/v3.0.4.html#fixed-fields-for-use-with-schema">explode value</a>.
     */
    public enum ParameterStyle
    {
        SIMPLE_PATH,
        FORM_EXPLODED_QUERY,
        FORM_UNEXPLODED_QUERY,
    }

    public static OpenApiParameterHandle from(
            Parameter resolvedParameter,
            SchemaIr parameterIr,
            CastPolicy castPolicy)
    {
        String name = resolvedParameter.getName();
        checkArgument(
                !Optional.ofNullable(resolvedParameter.getAllowReserved()).orElse(false),
                "Parameter '%s' uses unsupported 'allowReserved' keyword".formatted(
                        resolvedParameter.getName()));
        StyleEnum styleEnum = resolvedParameter.getStyle();
        ParameterStyle style = switch (resolvedParameter.getIn()) {
            case "path" -> {
                if (styleEnum == SIMPLE) {
                    yield SIMPLE_PATH;
                }
                throw new TrinoException(
                        OPENAPI_UNSUPPORTED_PARAMETER,
                        "Path parameter '%s' uses unsupported style '%s'".formatted(name, styleEnum.name()));
            }
            case "query" -> {
                boolean explode = resolvedParameter.getExplode();
                if (styleEnum == FORM && explode) {
                    yield FORM_EXPLODED_QUERY;
                }
                if (styleEnum == FORM) {
                    yield FORM_UNEXPLODED_QUERY;
                }
                throw new TrinoException(
                        OPENAPI_UNSUPPORTED_PARAMETER,
                        "Query parameter '%s' uses unsupported style '%s'".formatted(name, styleEnum.name()));
            }
            default -> throw new TrinoException(
                    OPENAPI_UNSUPPORTED_PARAMETER,
                    "Parameter '%s' uses unsupported location '%s'".formatted(name, resolvedParameter.getIn()));
        };
        TypeEncoder typeEncoder = TypeEncoder.from(parameterIr, castPolicy);
        return new OpenApiParameterHandle(
                name,
                style,
                typeEncoder,
                resolvedParameter.getRequired());
    }
}
