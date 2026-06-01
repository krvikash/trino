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
package io.trino.spi.connector.substitution;

/**
 * Connector-specific identity for a column, used by the materialization metastore
 * to persist column references across sessions and engine versions.
 * <p>
 * Implementations must:
 * <ul>
 *     <li>Be Jackson-serializable</li>
 *     <li>Implement {@link #equals(Object)} and {@link #hashCode()} with identity semantics as instances of this class are used as keys in a hash map.</li>
 *     <li>Follow cross-version compatibility rules when evolving fields</li>
 * </ul>
 */
public interface ConnectorColumnId
{
    @Override
    int hashCode();

    @Override
    boolean equals(Object other);

    /**
     * Format version of this identity; used to skip persisted entries with an incompatible format.
     * This should be bumped every time the implementation changes in a backwards-incompatible way.
     */
    ConnectorIdVersion version();
}
