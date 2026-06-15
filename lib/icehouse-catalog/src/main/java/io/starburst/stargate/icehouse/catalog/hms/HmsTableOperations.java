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
package io.starburst.stargate.icehouse.catalog.hms;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.icehouse.catalog.AbstractHiveMetastoreTableOperations;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveType;
import io.trino.metastore.Table;
import io.trino.plugin.iceberg.util.HiveSchemaUtil;
import io.trino.spi.TrinoException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;

// Copied from Trino's HiveMetastoreTableOperations
public final class HmsTableOperations
        extends AbstractHiveMetastoreTableOperations
{
    public HmsTableOperations(
            HiveMetastore metastore,
            FileIO fileIO,
            String schemaName,
            String tableName)
    {
        super(metastore, fileIO, schemaName, tableName);
    }

    @Override
    public void addMetastoreSpecificParameters(Table.Builder builder, TableMetadata newMetadata)
    {
        builder.setDataColumns(toHiveColumns(newMetadata.schema().columns()));
    }

    // Copied from AbstractIcebergTableOperations::toHiveColumns
    public static List<Column> toHiveColumns(List<Types.NestedField> columns)
    {
        try {
            return columns.stream()
                    .map(column -> new Column(
                            column.name(),
                            HiveType.fromTypeInfo(HiveSchemaUtil.convert(column.type())),
                            Optional.empty(),
                            ImmutableMap.of()))
                    .collect(toImmutableList());
        }
        catch (TrinoException e) {
            if (e.getErrorCode() == NOT_SUPPORTED.toErrorCode()) {
                Column dummyColumn = new Column("dummy", HIVE_STRING, Optional.empty(), ImmutableMap.of());
                return ImmutableList.of(dummyColumn);
            }
            throw e;
        }
    }
}
