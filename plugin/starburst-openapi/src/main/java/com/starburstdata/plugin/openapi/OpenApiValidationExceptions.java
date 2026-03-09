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

import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;

/**
 * An exception class to collect and truncate validation errors from an OpenApiSpec.
 */
public class OpenApiValidationExceptions
        extends RuntimeException
{
    private final List<FailedValidation> failedValidations;

    public OpenApiValidationExceptions(List<FailedValidation> failedValidations)
    {
        super(getMessage(failedValidations));
        this.failedValidations = failedValidations;
    }

    private static String getMessage(List<FailedValidation> failedValidations)
    {
        return failedValidations.stream()
                .map(failedValidation -> format("\"%s\"", failedValidation.getMessage()))
                .collect(Collectors.joining(", ", "There were a number of exceptions: ", ""));
    }

    public List<FailedValidation> getFailedValidations()
    {
        return failedValidations;
    }

    public interface FailedValidation
    {
        String getMessage();
    }

    record AmbiguousTableFunctionPath(
            String identifier,
            List<String> paths)
            implements FailedValidation
    {
        @Override
        public String getMessage()
        {
            return paths.stream()
                    .map(path -> format("\"%s\"", path))
                    .collect(Collectors.joining(
                            ", ",
                            "Paths ",
                            " all map to table function %s".formatted(identifier)));
        }
    }
}
