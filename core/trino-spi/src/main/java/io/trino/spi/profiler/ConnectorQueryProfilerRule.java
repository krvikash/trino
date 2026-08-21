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

import java.util.List;

public interface ConnectorQueryProfilerRule
{
    /**
     * Name of the connector this rule applies to. Must equal the connector name reported for a
     * table input, i.e. the name registered by the connector factory (for example {@code "iceberg"}).
     */
    String getConnectorName();

    /**
     * Analyze connector-specific input info, for query profiling purposes.
     *
     * @param connectorInfo the value produced by {@link io.trino.spi.connector.ConnectorMetadata#getInfo}
     * @return findings for the table input, or an empty list if none
     */
    List<ConnectorQueryProfilerFinding> analyze(Object connectorInfo);
}
