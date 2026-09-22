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
package io.trino.spi.profiler;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public record ConnectorQueryProfilerFinding(String description, Severity severity, Optional<String> recommendation)
{
    public ConnectorQueryProfilerFinding
    {
        requireNonNull(description, "description is null");
        requireNonNull(severity, "severity is null");
        requireNonNull(recommendation, "recommendation is null");
    }

    public ConnectorQueryProfilerFinding(String description)
    {
        this(description, Severity.WARNING, Optional.empty());
    }
}
