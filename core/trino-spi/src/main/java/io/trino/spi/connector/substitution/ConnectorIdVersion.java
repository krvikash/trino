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

import static java.util.Objects.requireNonNull;

/**
 * Versioned label for a connector's identity serialization format.
 * {@code key} names the identity type (e.g. the Java class simple name) and {@code id} is the
 * version number, bumped on every backward-incompatible format change.
 * The {@code key} is used to differentiate between different identity types in connectors that wrap other connectors.
 */
public record ConnectorIdVersion(String key, int id)
{
    public ConnectorIdVersion
    {
        requireNonNull(key, "key is null");
    }
}
