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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public class RedshiftSessionProperties
        implements SessionPropertiesProvider
{
    private static final String UNLOAD_ENABLED = "unload_enabled";
    private static final String UNSAFE_VARCHAR_PUSHDOWN_ENABLED = "unsafe_varchar_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public RedshiftSessionProperties(RedshiftConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        UNLOAD_ENABLED,
                        "Use UNLOAD for reading query results",
                        config.getUnloadLocation().isPresent(),
                        value -> {
                            if (value && config.getUnloadLocation().isEmpty()) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, "Cannot use UNLOAD when unload location is not configured");
                            }
                        },
                        false))
                .add(booleanProperty(
                        UNSAFE_VARCHAR_PUSHDOWN_ENABLED,
                        "Enable pushdown of VARCHAR predicates to Redshift. This may improve performance, but can cause incorrect results in case of trailing spaces",
                        config.isVarcharPushdownEnabled(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isUnloadEnabled(ConnectorSession session)
    {
        return session.getProperty(UNLOAD_ENABLED, Boolean.class);
    }

    public static boolean isVarcharPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(UNSAFE_VARCHAR_PUSHDOWN_ENABLED, Boolean.class);
    }
}
