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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class ObjectStoreViewProperties
{
    private final ImmutableMap<String, PropertyMetadata<?>> properties;

    @Inject
    public ObjectStoreViewProperties(
            @ForHive Connector hiveConnector,
            @ForIceberg Connector icebergConnector,
            @ForDelta Connector deltaConnector,
            @ForHudi Connector hudiConnector)
    {
        requireNonNull(hiveConnector, "hiveConnector is null");
        requireNonNull(icebergConnector, "icebergConnector is null");
        requireNonNull(deltaConnector, "deltaConnector is null");
        requireNonNull(hudiConnector, "hudiConnector is null");

        ImmutableMap.Builder<String, PropertyMetadata<?>> properties = ImmutableMap.builder();
        for (PropertyMetadata<?> property : hiveConnector.getViewProperties()) {
            properties.put(property.getName(), property);
        }
        for (PropertyMetadata<?> property : icebergConnector.getViewProperties()) {
            if (property.getName().equals("location")) {
                // Only REST and JDBC catalogs support location view property. This connector doesn't support those catalogs
                continue;
            }
            properties.put(property.getName(), property);
        }
        verify(deltaConnector.getViewProperties().isEmpty(), "View properties in Delta Lake connector must be empty");
        verify(hudiConnector.getViewProperties().isEmpty(), "View properties in Hudi connector must be empty");
        this.properties = properties.buildOrThrow();
    }

    public List<PropertyMetadata<?>> getProperties()
    {
        return properties.values().asList();
    }
}
