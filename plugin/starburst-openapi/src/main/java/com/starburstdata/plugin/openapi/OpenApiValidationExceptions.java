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

import io.trino.spi.TrinoException;

import java.util.List;
import java.util.stream.Collectors;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;

/**
 * An exception class to collect errors from an OpenApiSpec.
 */
public class OpenApiValidationExceptions
        extends TrinoException
{
    private final List<Exception> specificationExceptions;

    public OpenApiValidationExceptions(List<Exception> specificationExceptions)
    {
        super(CONFIGURATION_INVALID, getMessage(specificationExceptions), specificationExceptions.getFirst());
        this.specificationExceptions = specificationExceptions;
        specificationExceptions.stream().skip(1).forEach(this::addSuppressed);
    }

    private static String getMessage(List<Exception> specificationExceptions)
    {
        return specificationExceptions.stream()
                .map(Exception::getMessage)
                .collect(Collectors.joining("\n * ", "There were a number of exceptions: \n * ", ""));
    }

    public List<Exception> getSpecificationExceptions()
    {
        return specificationExceptions;
    }
}
