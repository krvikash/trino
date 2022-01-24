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

import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.ConfigHidden;
import io.trino.plugin.iceberg.IcebergFileFormat;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;

public class ObjectStoreConfig
{
    private TableType tableType = TableType.HIVE;
    private int maxMetadataQueriesProcessingThreads = 32; // This is for IO, so default value not based on number of cores.
    private IcebergFileFormat defaultIcebergFileFormat = PARQUET;
    private boolean isIcebergRestCatalogUsed;
    private boolean galaxyLocationSecurityEnabled;

    @NotNull
    public TableType getTableType()
    {
        return tableType;
    }

    @CanIgnoreReturnValue
    @Config("object-store.table-type")
    public ObjectStoreConfig setTableType(TableType tableType)
    {
        this.tableType = tableType;
        return this;
    }

    @Min(1)
    public int getMaxMetadataQueriesProcessingThreads()
    {
        return maxMetadataQueriesProcessingThreads;
    }

    @Config("object-store.information-schema-queries-threads")
    public ObjectStoreConfig setMaxMetadataQueriesProcessingThreads(int maxMetadataQueriesProcessingThreads)
    {
        this.maxMetadataQueriesProcessingThreads = maxMetadataQueriesProcessingThreads;
        return this;
    }

    public IcebergFileFormat getDefaultIcebergFileFormat()
    {
        return defaultIcebergFileFormat;
    }

    @Config("object-store.iceberg-default-file-format")
    public ObjectStoreConfig setDefaultIcebergFileFormat(IcebergFileFormat defaultIcebergFileFormat)
    {
        this.defaultIcebergFileFormat = defaultIcebergFileFormat;
        return this;
    }

    public boolean isIcebergRestCatalogUsed()
    {
        return isIcebergRestCatalogUsed;
    }

    @ConfigDescription("Must be set to true when ObjectStore connector is configured with Iceberg REST catalog")
    @Config("object-store.iceberg-rest-catalog-used")
    public ObjectStoreConfig setIcebergRestCatalogUsed(boolean icebergRestCatalogUsed)
    {
        isIcebergRestCatalogUsed = icebergRestCatalogUsed;
        return this;
    }

    public boolean isGalaxyLocationSecurityEnabled()
    {
        return galaxyLocationSecurityEnabled;
    }

    // TODO Rename to object-store.galaxy.location-security.enabled
    @Config("galaxy.location-security.enabled")
    @ConfigHidden
    public ObjectStoreConfig setGalaxyLocationSecurityEnabled(boolean galaxyLocationSecurityEnabled)
    {
        this.galaxyLocationSecurityEnabled = galaxyLocationSecurityEnabled;
        return this;
    }
}
