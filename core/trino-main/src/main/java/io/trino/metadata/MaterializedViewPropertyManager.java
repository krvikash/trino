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
package io.trino.metadata;

import io.trino.connector.CatalogHandle;
import io.trino.connector.CatalogServiceProvider;
import io.trino.spi.session.PropertyMetadata;

import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.StandardErrorCode.INVALID_MATERIALIZED_VIEW_PROPERTY;
import static java.util.Objects.requireNonNull;

public class MaterializedViewPropertyManager
        extends AbstractCatalogPropertyManager
{
    public static final String SUBSTITUTION_ENABLED = "substitution_enabled";

    public MaterializedViewPropertyManager(boolean substitutionSupportEnabled, CatalogServiceProvider<Map<String, PropertyMetadata<?>>> connectorProperties)
    {
        super("materialized view", INVALID_MATERIALIZED_VIEW_PROPERTY, new GlobalGateWrapper(substitutionSupportEnabled, connectorProperties));
    }

    public static boolean isSubstitutionEnabled(Map<String, Object> properties)
    {
        return (Boolean) properties.getOrDefault(SUBSTITUTION_ENABLED, false);
    }

    /**
     * Filters out {@code substitution_enabled} from every connector's property map when the
     * engine-wide MV substitution support flag is off. Connectors are free to declare the
     * property; the engine has final say on whether it is accepted by users.
     * The goal is to avoid misleading users, when the feature is disabled, that MV will be used for substitution because
     * the MV creation with the `substitution_enabled` succeeded.
     */
    private static class GlobalGateWrapper
            implements CatalogServiceProvider<Map<String, PropertyMetadata<?>>>
    {
        private final boolean substitutionSupportEnabled;
        private final CatalogServiceProvider<Map<String, PropertyMetadata<?>>> delegate;

        private GlobalGateWrapper(boolean substitutionSupportEnabled, CatalogServiceProvider<Map<String, PropertyMetadata<?>>> delegate)
        {
            this.substitutionSupportEnabled = substitutionSupportEnabled;
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public Map<String, PropertyMetadata<?>> getService(CatalogHandle catalogHandle)
        {
            Map<String, PropertyMetadata<?>> connectorProperties = delegate.getService(catalogHandle);
            if (substitutionSupportEnabled || !connectorProperties.containsKey(SUBSTITUTION_ENABLED)) {
                return connectorProperties;
            }
            return connectorProperties.entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(SUBSTITUTION_ENABLED))
                    .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        }
    }
}
