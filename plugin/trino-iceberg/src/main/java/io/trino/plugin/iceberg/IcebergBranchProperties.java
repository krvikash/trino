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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.plugin.base.session.PropertyMetadataUtil.durationProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;
import static io.trino.spi.session.PropertyMetadata.longProperty;

public class IcebergBranchProperties
{
    public enum ReferenceType
    {
        BRANCH,
        TAG,
    }

    private static final String TYPE_PROPERTY = "type";
    private static final String SNAPSHOT_ID_PROPERTY = "snapshot_id";
    private static final String RETENTION_PROPERTY = "retention";

    private final List<PropertyMetadata<?>> branchProperties;

    @Inject
    public IcebergBranchProperties()
    {
        branchProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(enumProperty(
                        TYPE_PROPERTY,
                        "Reference type to create",
                        ReferenceType.class,
                        ReferenceType.BRANCH,
                        false))
                .add(longProperty(
                        SNAPSHOT_ID_PROPERTY,
                        "Snapshot ID to tag",
                        null,
                        false))
                .add(durationProperty(
                        RETENTION_PROPERTY,
                        "Retention period for tags",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getBranchProperties()
    {
        return branchProperties;
    }

    public static ReferenceType getReferenceType(Map<String, Object> properties)
    {
        return (ReferenceType) properties.getOrDefault(TYPE_PROPERTY, ReferenceType.BRANCH);
    }

    public static OptionalLong getSnapshotId(Map<String, Object> properties)
    {
        Long snapshotId = (Long) properties.get(SNAPSHOT_ID_PROPERTY);
        return snapshotId == null ? OptionalLong.empty() : OptionalLong.of(snapshotId);
    }

    public static Optional<Duration> getRetention(Map<String, Object> properties)
    {
        return Optional.ofNullable((Duration) properties.get(RETENTION_PROPERTY));
    }
}
