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
