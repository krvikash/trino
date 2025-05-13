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
package io.trino.plugin.hive.metastore.unity;

import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.TableType;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Map;

import static java.util.Objects.requireNonNull;

// TODO refactor this with official databricks sdk support of createTable
//  https://starburstdata.atlassian.net/browse/CONNECT-592
//  based on com.databricks.sdk.service.catalog.TableInfo to satisfy private-preview databricks sdk api
public final class CreateTable
{
    @JsonProperty("catalog_name")
    private final String catalogName;
    @JsonProperty("schema_name")
    private final String schemaName;
    @JsonProperty("name")
    private final String name;
    @JsonProperty("table_type")
    private final TableType tableType;
    @JsonProperty("data_source_format")
    private final DataSourceFormat dataSourceFormat;
    @JsonProperty("owner")
    private final String owner;
    @JsonProperty("storage_location")
    private final String storageLocation;
    @JsonProperty("properties")
    private final Map<String, String> properties;
    @JsonProperty("columns")
    private final Collection<ColumnInfo> columns;

    public CreateTable(
            String catalogName,
            String schemaName,
            String name,
            TableType tableType,
            DataSourceFormat dataSourceFormat,
            String owner,
            String storageLocation,
            Map<String, String> properties,
            Collection<ColumnInfo> columns)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.name = requireNonNull(name, "name is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.dataSourceFormat = requireNonNull(dataSourceFormat, "dataSourceFormat is null");
        this.owner = requireNonNull(owner, "owner is null");
        this.storageLocation = requireNonNull(storageLocation, "storageLocation is null");
        this.properties = requireNonNull(properties, "properties is null");
        this.columns = requireNonNull(columns, "columns is null");
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getName()
    {
        return name;
    }

    public TableType getTableType()
    {
        return tableType;
    }

    public DataSourceFormat getDataSourceFormat()
    {
        return dataSourceFormat;
    }

    public String getOwner()
    {
        return owner;
    }

    public String getStorageLocation()
    {
        return storageLocation;
    }

    public Map<String, String> getProperties()
    {
        return properties;
    }

    public Collection<ColumnInfo> getColumns()
    {
        return columns;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static final class Builder
    {
        private String catalogName;
        private String schemaName;
        private String name;
        private TableType tableType;
        private DataSourceFormat dataSourceFormat;
        private String owner;
        private String storageLocation;
        private Map<String, String> properties;
        private Collection<ColumnInfo> columns;

        public Builder() {}

        public Builder setCatalogName(String catalogName)
        {
            this.catalogName = catalogName;
            return this;
        }

        public Builder setSchemaName(String schemaName)
        {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public Builder setTableType(TableType tableType)
        {
            this.tableType = tableType;
            return this;
        }

        public Builder setDataSourceFormat(DataSourceFormat dataSourceFormat)
        {
            this.dataSourceFormat = dataSourceFormat;
            return this;
        }

        public Builder setOwner(String owner)
        {
            this.owner = owner;
            return this;
        }

        public Builder setStorageLocation(String storageLocation)
        {
            this.storageLocation = storageLocation;
            return this;
        }

        public Builder setProperties(Map<String, String> properties)
        {
            this.properties = properties;
            return this;
        }

        public Builder setColumns(Collection<ColumnInfo> columns)
        {
            this.columns = columns;
            return this;
        }

        public CreateTable build()
        {
            return new CreateTable(
                    catalogName,
                    schemaName,
                    name,
                    tableType,
                    dataSourceFormat,
                    owner,
                    storageLocation,
                    properties,
                    columns);
        }
    }
}
