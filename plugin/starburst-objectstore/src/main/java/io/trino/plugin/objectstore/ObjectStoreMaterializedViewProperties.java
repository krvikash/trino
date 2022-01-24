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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.IcebergTableProperties;
import io.trino.spi.connector.Connector;
import io.trino.spi.session.PropertyMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class ObjectStoreMaterializedViewProperties
{
    private final ImmutableMap<String, PropertyMetadata<?>> properties;

    // This constant overrides the default value and also disallows the user to set the property
    private static final Map<String, Object> ICEBERG_PROPERTY_OVERRIDES = ImmutableMap.<String, Object>builder()
            .put(IcebergTableProperties.FILE_FORMAT_PROPERTY, IcebergFileFormat.PARQUET)
            .put(IcebergTableProperties.FORMAT_VERSION_PROPERTY, 2)
            .buildOrThrow();

    private static final Set<String> ICEBERG_PROPERTY_OMISSIONS = ImmutableSet.<String>builder()
            .add(IcebergTableProperties.LOCATION_PROPERTY)
            .add(IcebergTableProperties.OBJECT_STORE_LAYOUT_ENABLED_PROPERTY)
            .add(IcebergTableProperties.DATA_LOCATION_PROPERTY)
            .build();

    @Inject
    public ObjectStoreMaterializedViewProperties(@ForIceberg Connector icebergConnector)
    {
        Map<String, PropertyMetadata<?>> properties = new HashMap<>();
        Set<String> overriddenOrOmittedProperties = ImmutableSet.<String>builder()
                .addAll(ICEBERG_PROPERTY_OMISSIONS)
                .addAll(ICEBERG_PROPERTY_OVERRIDES.keySet())
                .build();

        for (PropertyMetadata<?> property : icebergConnector.getMaterializedViewProperties()) {
            if (overriddenOrOmittedProperties.contains(property.getName())) {
                // properties in deny list are override or removed later
                continue;
            }
            properties.put(property.getName(), property);
        }
        this.properties = ImmutableMap.copyOf(properties);
    }

    public List<PropertyMetadata<?>> getProperties()
    {
        return this.properties.values().asList();
    }

    public static Map<String, Object> addIcebergPropertyOverrides(Map<String, Object> properties)
    {
        return ImmutableMap.<String, Object>builder()
                .putAll(properties)
                .putAll(ICEBERG_PROPERTY_OVERRIDES)
                .buildOrThrow();
    }

    public static Map<String, Object> removeOverriddenOrRemovedIcebergProperties(Map<String, Object> properties)
    {
        return properties.entrySet()
                .stream()
                .filter(entry -> !ICEBERG_PROPERTY_OMISSIONS.contains(entry.getKey()) && !ICEBERG_PROPERTY_OVERRIDES.containsKey(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
