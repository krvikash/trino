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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.plugin.mongodb.MongoClientConfig.SamplingOrder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public final class MongoSessionProperties
        implements SessionPropertiesProvider
{
    private static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";
    public static final String DYNAMIC_FILTERING_WAIT_TIMEOUT = "dynamic_filtering_wait_timeout";
    public static final String SAMPLING_COUNT = "sampling_count";
    public static final String SAMPLING_ORDER = "sampling_order";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public MongoSessionProperties(MongoClientConfig mongoConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Read only required fields from a row type",
                        mongoConfig.isProjectionPushdownEnabled(),
                        false))
                .add(durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIMEOUT,
                        "Duration to wait for completion of dynamic filters",
                        mongoConfig.getDynamicFilteringWaitTimeout(),
                        false))
                .add(integerProperty(
                        SAMPLING_COUNT,
                        "How many documents are used for field type inference",
                        mongoConfig.getSamplingCount(),
                        value -> {
                            if (value < 1) {
                                throw new TrinoException(INVALID_SESSION_PROPERTY, "Sampling count must be a positive integer: %s".formatted(value));
                            }
                        },
                        false))
                .add(enumProperty(
                        SAMPLING_ORDER,
                        "Which records should be read for sampling",
                        SamplingOrder.class,
                        mongoConfig.getSamplingOrder().orElse(null),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static Duration getDynamicFilteringWaitTimeout(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_WAIT_TIMEOUT, Duration.class);
    }

    public static int getSamplingCount(ConnectorSession session)
    {
        return session.getProperty(SAMPLING_COUNT, Integer.class);
    }

    public static Optional<SamplingOrder> getSamplingOrder(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(SAMPLING_ORDER, SamplingOrder.class));
    }
}
