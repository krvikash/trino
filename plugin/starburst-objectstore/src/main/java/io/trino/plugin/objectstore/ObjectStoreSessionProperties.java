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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.TimeZoneKey;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.objectstore.FeatureExposure.UNDEFINED;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDefaultValue.IGNORE_DEFAULT_VALUE;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDefaultValue.VERIFY_DEFAULT_VALUE;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.IGNORE_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.VerifyDescription.VERIFY_DESCRIPTION;
import static io.trino.plugin.objectstore.PropertyMetadataValidation.verifyPropertyMetadata;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

public class ObjectStoreSessionProperties
        implements SessionPropertiesProvider
{
    private final List<PropertyMetadata<?>> sessionProperties;
    private final Multimap<String, TableType> maskedProperties;
    private final Table<String, TableType, Optional<Object>> defaultPropertyValue;

    @Inject
    public ObjectStoreSessionProperties(DelegateConnectors delegates, FeatureExposures featureExposures)
    {
        requireNonNull(featureExposures, "featureExposures is null");

        Set<String> ignoredDescriptions = ImmutableSet.<String>builder()
                .add("compression_codec")
                .add("projection_pushdown_enabled")
                .add("timestamp_precision")
                .add("minimum_assigned_split_weight")
                .add("query_partition_filter_required")
                .add("ignore_absent_partitions")
                .build();

        Table<String, TableType, PropertyMetadata<?>> delegateProperties = HashBasedTable.create();
        ImmutableMultimap.Builder<String, TableType> maskedProperties = ImmutableMultimap.builder();
        ImmutableTable.Builder<String, TableType, Optional<Object>> defaultPropertyValue = ImmutableTable.builder();
        Table<TableType, String, FeatureExposure> sessionExposures = HashBasedTable.create(featureExposures.sessionExposureDecisions());
        delegates.byType().forEach((type, connector) -> {
            for (PropertyMetadata<?> property : connector.getSessionProperties()) {
                String name = property.getName();
                switch (requireNonNullElse(sessionExposures.remove(type, name), UNDEFINED)) {
                    case INACCESSIBLE -> {
                        maskedProperties.put(name, type);
                        defaultPropertyValue.put(name, type, Optional.ofNullable(property.getDefaultValue()));
                    }
                    case UNDEFINED -> throw new IllegalStateException("Unknown session property provided by %s: %s".formatted(type, name));
                    case EXPOSED -> delegateProperties.put(name, type, property);
                }
            }
        });

        ImmutableList.Builder<PropertyMetadata<?>> sessionProperties = ImmutableList.builder();
        for (Map.Entry<String, Map<TableType, PropertyMetadata<?>>> propertyCandidatesEntry : delegateProperties.rowMap().entrySet()) {
            String name = propertyCandidatesEntry.getKey();
            Map<TableType, PropertyMetadata<?>> propertiesOfName = propertyCandidatesEntry.getValue();
            PropertyMetadataValidation.VerifyDescription verifyDescription = ignoredDescriptions.contains(name) ? IGNORE_DESCRIPTION : VERIFY_DESCRIPTION;
            PropertyMetadata<?> first = Stream.of(TableType.values())
                    .map(propertiesOfName::get)
                    .filter(Objects::nonNull)
                    .findFirst().orElseThrow();

            boolean commonDefaultValue = propertiesOfName.values().stream()
                    .map(PropertyMetadata::getDefaultValue)
                    .distinct()
                    .count() == 1;

            if (commonDefaultValue) {
                for (PropertyMetadata<?> existing : propertiesOfName.values()) {
                    verifyPropertyMetadata(
                            first,
                            existing,
                            VERIFY_DEFAULT_VALUE, // technically redundant now
                            verifyDescription);
                }
                sessionProperties.add(first);
            }
            else {
                for (Map.Entry<TableType, PropertyMetadata<?>> entry : propertiesOfName.entrySet()) {
                    verifyPropertyMetadata(
                            first,
                            entry.getValue(),
                            IGNORE_DEFAULT_VALUE,
                            verifyDescription);
                    defaultPropertyValue.put(name, entry.getKey(), Optional.ofNullable(entry.getValue().getDefaultValue()));
                }
                sessionProperties.add(first.withDefault(null));
            }
        }

        List<PropertyMetadata<?>> wrappedDelegateProperties = sessionProperties.build().stream()
                .map(ObjectStoreSessionProperties::toWrapped)
                .collect(toImmutableList());

        List<PropertyMetadata<?>> objectStoreProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .build();

        this.sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .addAll(wrappedDelegateProperties)
                .addAll(objectStoreProperties)
                .build();
        this.maskedProperties = maskedProperties.build();
        this.defaultPropertyValue = defaultPropertyValue.buildOrThrow();
    }

    public ConnectorSession unwrap(TableType forType, ConnectorSession session)
    {
        return new DelegateSession(forType, session);
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    private static <T> PropertyMetadata<WrappedPropertyValue> toWrapped(PropertyMetadata<T> propertyMetadata)
    {
        // The WrappedValue is a wrapper type making sure we unwrap ConnectorSession properties explicitly
        // when leaving ObjectStore connector code into delegate connector OR we fail loudly.
        return propertyMetadata.convert(
                WrappedPropertyValue.class,
                WrappedPropertyValue::new,
                wrappedPropertyValue -> propertyMetadata.getJavaType().cast(wrappedPropertyValue.value()));
    }

    private class DelegateSession
            implements ConnectorSession
    {
        private final TableType forType;
        private final ConnectorSession baseSession; // from engine

        public DelegateSession(TableType forType, ConnectorSession baseSession)
        {
            this.forType = requireNonNull(forType, "forType is null");
            this.baseSession = requireNonNull(baseSession, "baseSession is null");
        }

        @Override
        public String getQueryId()
        {
            return baseSession.getQueryId();
        }

        @Override
        public Optional<String> getSource()
        {
            return baseSession.getSource();
        }

        @Override
        public String getUser()
        {
            return baseSession.getUser();
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return baseSession.getIdentity();
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return baseSession.getTimeZoneKey();
        }

        @Override
        public Locale getLocale()
        {
            return baseSession.getLocale();
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return baseSession.getTraceToken();
        }

        @Override
        public Instant getStart()
        {
            return baseSession.getStart();
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            Object connectorValue = null;
            if (!maskedProperties.containsEntry(name, forType)) {
                WrappedPropertyValue wrappedPropertyValue = baseSession.getProperty(name, WrappedPropertyValue.class);
                connectorValue = wrappedPropertyValue.value();
            }
            if (connectorValue == null) {
                Optional<Object> defaultValue = defaultPropertyValue.row(name).getOrDefault(forType, Optional.empty());
                connectorValue = defaultValue.orElse(null);
            }
            return type.cast(connectorValue);
        }

        @Override
        public <T extends ConnectorSession> Optional<T> unwrap(Class<T> type)
        {
            if (type.isInstance(this)) {
                return Optional.of(type.cast(this));
            }
            return baseSession.unwrap(type);
        }
    }
}
