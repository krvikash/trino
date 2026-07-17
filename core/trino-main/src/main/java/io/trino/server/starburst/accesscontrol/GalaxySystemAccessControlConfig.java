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
package io.trino.server.starburst.accesscontrol;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.starburst.stargate.id.SharedSchemaNameAndAccepted;
import io.trino.spi.connector.SchemaTableName;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Config for {@link GalaxyAccessControl} subsystem.
 * {@link GalaxyAccessControlConfig} but bound in the main context only (not twice).
 */
public class GalaxySystemAccessControlConfig
{
    public static final Duration DEFAULT_CACHE_EXPIRATION = Duration.ofMinutes(10);
    private int backgroundProcessingThreads = 8;
    private int visibilityBatchSize = 2_000;
    private boolean systemRuntimeFilterOtherUsers;
    // Currently, we allow at most 60 concurrent queries (20 queries and 40 "data definition"), this value is with some margin.
    private int expectedQueryParallelism = 100;
    private Optional<Duration> expireAfterWriteDuration = Optional.of(DEFAULT_CACHE_EXPIRATION);
    private Set<String> readOnlyCatalogs = ImmutableSet.of();
    private Optional<Map<String, SharedSchemaNameAndAccepted>> sharedCatalogSchemaNames = Optional.empty();
    private boolean galaxyEntityPrivilegesEnabled;
    private SetMultimap<String, SchemaTableName> alwaysVisibleCatalogSystemTables = ImmutableSetMultimap.of();
    private AccessControlMode accessControlMode = AccessControlMode.GALAXY;

    public enum AccessControlMode
    {
        GALAXY,
        SEP;

        public static AccessControlMode fromString(String value)
        {
            for (AccessControlMode mode : AccessControlMode.values()) {
                if (mode.name().equalsIgnoreCase(value)) {
                    return mode;
                }
            }
            throw new IllegalArgumentException("Invalid access control mode: " + value);
        }
    }

    @Min(0)
    public int getBackgroundProcessingThreads()
    {
        return backgroundProcessingThreads;
    }

    @Config("galaxy.access-control-background-threads")
    public GalaxySystemAccessControlConfig setBackgroundProcessingThreads(int backgroundProcessingThreads)
    {
        this.backgroundProcessingThreads = backgroundProcessingThreads;
        return this;
    }

    @Min(1)
    public int getVisibilityBatchSize()
    {
        return visibilityBatchSize;
    }

    @Config("galaxy.visibility-batch-size")
    public GalaxySystemAccessControlConfig setVisibilityBatchSize(int visibilityBatchSize)
    {
        this.visibilityBatchSize = visibilityBatchSize;
        return this;
    }

    @Min(1)
    public int getExpectedQueryParallelism()
    {
        return expectedQueryParallelism;
    }

    @Config("galaxy.expected-query-parallelism")
    @ConfigDescription("Expected query parallelism, should be the sum of hardConcurrencyLimit of all resource groups")
    public GalaxySystemAccessControlConfig setExpectedQueryParallelism(int expectedQueryParallelism)
    {
        this.expectedQueryParallelism = expectedQueryParallelism;
        return this;
    }

    public boolean getSystemRuntimeFilterOtherUsers()
    {
        return systemRuntimeFilterOtherUsers;
    }

    @Config("galaxy.system-runtime-filter-other-users")
    @ConfigDescription(
            """
            An optional flag for limiting the ability for *users* to view the queries of other users.
            By default, because Galaxy is role-based, filtering queries via system.runtime.queries or viewing a query
            will be allowed across different users as long as the query is in the role's active role set.
            When set to true, only the user's queries will be visible.
            """)
    public GalaxySystemAccessControlConfig setSystemRuntimeFilterOtherUsers(boolean systemRuntimeFilterOtherUsers)
    {
        this.systemRuntimeFilterOtherUsers = systemRuntimeFilterOtherUsers;
        return this;
    }

    @NotNull
    public Optional<Duration> getPermissionsCacheExpireAfterWriteDuration()
    {
        return expireAfterWriteDuration;
    }

    @Config("galaxy.permissions-cache-expiration")
    public GalaxySystemAccessControlConfig setPermissionsCacheExpireAfterWriteDuration(String duration)
    {
        this.expireAfterWriteDuration = Optional.of(Duration.parse(duration));
        return this;
    }

    @NotNull
    public Set<String> getReadOnlyCatalogs()
    {
        return readOnlyCatalogs;
    }

    public GalaxySystemAccessControlConfig setReadOnlyCatalogs(Set<String> readOnlyCatalogs)
    {
        this.readOnlyCatalogs = ImmutableSet.copyOf(requireNonNull(readOnlyCatalogs));
        return this;
    }

    @Config("galaxy.read-only-catalogs")
    public GalaxySystemAccessControlConfig setReadOnlyCatalogs(String catalogNames)
    {
        this.readOnlyCatalogs = Splitter.on(",").trimResults().omitEmptyStrings().splitToStream(catalogNames)
                .collect(toImmutableSet());
        return this;
    }

    @NotNull
    public Optional<Map<String, SharedSchemaNameAndAccepted>> getSharedCatalogSchemaNames()
    {
        return sharedCatalogSchemaNames;
    }

    public GalaxySystemAccessControlConfig setSharedCatalogSchemaNames(Optional<Map<String, SharedSchemaNameAndAccepted>> sharedCatalogSchemaNames)
    {
        this.sharedCatalogSchemaNames = requireNonNull(sharedCatalogSchemaNames, "sharedCatalogSchemaNames is null");
        return this;
    }

    @Config("galaxy.shared-catalog-schemas")
    public GalaxySystemAccessControlConfig setSharedCatalogSchemaNames(String sharedCatalogSchemaNames)
    {
        if (sharedCatalogSchemaNames.isBlank()) {
            this.sharedCatalogSchemaNames = Optional.empty();
        }
        else {
            Map<String, String> splitStrings = ImmutableMap.copyOf(Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("->").split(sharedCatalogSchemaNames));
            ImmutableMap.Builder<String, SharedSchemaNameAndAccepted> builder = ImmutableMap.builder();
            splitStrings.forEach((catalogName, value) -> builder.put(catalogName, decodeSharedSchemaString(value)));
            this.sharedCatalogSchemaNames = Optional.of(builder.buildOrThrow());
        }
        return this;
    }

    public boolean isGalaxyEntityPrivilegesEnabled()
    {
        return galaxyEntityPrivilegesEnabled;
    }

    @Config("galaxy.entity-privileges.enabled")
    public GalaxySystemAccessControlConfig setGalaxyEntityPrivilegesEnabled(boolean galaxyEntityPrivilegesEnabled)
    {
        this.galaxyEntityPrivilegesEnabled = galaxyEntityPrivilegesEnabled;
        return this;
    }

    /**
     * The format of the string:
     * schemaName if accepted
     * *schemaName if not accepted and schemaName is non-null
     * * if not accepted and schemaName is null
     */
    private static SharedSchemaNameAndAccepted decodeSharedSchemaString(String value)
    {
        checkArgument(value != null && !value.isEmpty(), "value %s is null or empty", value);
        if ("*".equals(value)) {
            return new SharedSchemaNameAndAccepted(null, false);
        }
        if (value.startsWith("*")) {
            return new SharedSchemaNameAndAccepted(value.substring(1), false);
        }
        return new SharedSchemaNameAndAccepted(value, true);
    }

    @NotNull
    public SetMultimap<String, SchemaTableName> getAlwaysVisibleCatalogSystemTables()
    {
        return alwaysVisibleCatalogSystemTables;
    }

    public GalaxySystemAccessControlConfig setAlwaysVisibleCatalogSystemTables(SetMultimap<String, SchemaTableName> alwaysVisibleCatalogSystemTables)
    {
        this.alwaysVisibleCatalogSystemTables = alwaysVisibleCatalogSystemTables;
        return this;
    }

    @Config("galaxy.catalog-always-visible-system-tables")
    public GalaxySystemAccessControlConfig setAlwaysVisibleCatalogSystemTables(String alwaysVisibleCatalogSystemTables)
    {
        if (alwaysVisibleCatalogSystemTables == null || alwaysVisibleCatalogSystemTables.isBlank()) {
            this.alwaysVisibleCatalogSystemTables = ImmutableSetMultimap.of();
        }
        else {
            List<String> catalogTableMappings = Splitter.on(",").trimResults().omitEmptyStrings().splitToList(alwaysVisibleCatalogSystemTables);
            ImmutableSetMultimap.Builder<String, SchemaTableName> builder = ImmutableSetMultimap.builder();
            catalogTableMappings.forEach(catalogTableMapping -> {
                String[] catalogAndTableSchema = catalogTableMapping.split("->");
                String catalogName = catalogAndTableSchema[0];
                String schemaNameDotTableName = catalogAndTableSchema[1];
                builder.put(catalogName, new SchemaTableName(schemaNameDotTableName.split("\\.")[0], schemaNameDotTableName.split("\\.")[1]));
            });
            this.alwaysVisibleCatalogSystemTables = builder.build();
        }
        return this;
    }

    @NotNull
    public AccessControlMode getAccessControlMode()
    {
        return accessControlMode;
    }

    @Config("galaxy.access-control-mode")
    public GalaxySystemAccessControlConfig setAccessControlMode(String accessControlMode)
    {
        this.accessControlMode = AccessControlMode.fromString(accessControlMode);
        return this;
    }
}
