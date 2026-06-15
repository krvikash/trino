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
package io.starburst.stargate.icehouse.catalog;

import com.google.common.collect.ImmutableMap;
import io.starburst.stargate.icehouse.exception.InvalidMetadataException;
import io.starburst.stargate.icehouse.exception.TableNotFoundException;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.spi.TrinoException;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;

import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.CURRENT_SNAPSHOT_ID;
import static org.apache.iceberg.TableProperties.CURRENT_SNAPSHOT_TIMESTAMP;

public abstract class AbstractHiveMetastoreTableOperations
        extends AbstractTableOperations
{
    private final HiveMetastore metastore;

    protected AbstractHiveMetastoreTableOperations(
            HiveMetastore metastore,
            FileIO fileIO,
            String schemaName,
            String tableName)
    {
        super(fileIO, schemaName, tableName);
        this.metastore = requireNonNull(metastore, "metastore is null");
    }

    @Override
    protected String getMetadataLocation()
    {
        Table table = getHmsTable();

        String metadataLocation = table.getParameters().get(METADATA_LOCATION_PROP);
        if (metadataLocation == null) {
            throw new InvalidMetadataException(format("Table is missing [%s] property: %s.%s", METADATA_LOCATION_PROP, schemaName, tableName));
        }

        return metadataLocation;
    }

    @Override
    protected void commitMetadata(TableMetadata newMetadata, String newMetadataLocation, String previousMetadataLocation)
    {
        Table currentTable = getHmsTable();

        // Verify current metadata location matches what we expect
        String tableMetadataLocation = currentTable.getParameters().get(METADATA_LOCATION_PROP);
        if (previousMetadataLocation != null && !previousMetadataLocation.equals(tableMetadataLocation)) {
            throw new CommitFailedException(
                    "Metadata location [%s] is not same as table metadata location [%s] for %s.%s",
                    previousMetadataLocation,
                    tableMetadataLocation,
                    schemaName,
                    tableName);
        }

        // Build updated table with new metadata location
        Table.Builder updatedTableBuilder = Table.builder(currentTable)
                .withStorage(storage -> storage.setLocation(newMetadata.location()))
                .setParameter(METADATA_LOCATION_PROP, newMetadataLocation)
                .setParameter(PREVIOUS_METADATA_LOCATION_PROP, previousMetadataLocation)
                .setParameter(TABLE_COMMENT, Optional.ofNullable(newMetadata.properties().get(TABLE_COMMENT)));

        if (newMetadata.currentSnapshot() != null) {
            updatedTableBuilder
                    .setParameter(CURRENT_SNAPSHOT_ID, String.valueOf(newMetadata.currentSnapshot().snapshotId()))
                    .setParameter(CURRENT_SNAPSHOT_TIMESTAMP, String.valueOf(newMetadata.currentSnapshot().timestampMillis()));
        }

        addMetastoreSpecificParameters(updatedTableBuilder, newMetadata);

        // Build environment context for HMS optimistic concurrency control
        Map<String, String> environmentContext = buildEnvironmentContext(previousMetadataLocation);

        try {
            metastore.replaceTable(
                    schemaName,
                    tableName,
                    updatedTableBuilder.build(),
                    NO_PRIVILEGES,
                    environmentContext);
        }
        catch (io.trino.spi.connector.TableNotFoundException e) {
            throw new TableNotFoundException(format("Cannot commit table update for hive metastore table %s.%s", schemaName, tableName));
        }
        catch (TrinoException e) {
            handleTrinoException(e);
        }
        catch (RuntimeException e) {
            // Cannot determine whether the `replaceTable` operation was successful,
            // regardless of the exception thrown (e.g. : timeout exception) or it actually failed
            throw new CommitStateUnknownException(e);
        }
    }

    /**
     * Hook for subclasses to apply additional customizations to the table builder before commit.
     * Default implementation is a no-op.
     */
    protected void addMetastoreSpecificParameters(Table.Builder builder, TableMetadata newMetadata) {}

    protected void handleTrinoException(TrinoException e)
    {
        throw new CommitStateUnknownException(e);
    }

    protected Table getHmsTable()
    {
        return metastore.getTable(schemaName, tableName)
                .orElseThrow(() -> new TableNotFoundException(format("HMS table not found: %s.%s", schemaName, tableName)));
    }

    private static Map<String, String> buildEnvironmentContext(String metadataLocation)
    {
        if (metadataLocation == null) {
            return ImmutableMap.of();
        }
        // HMS uses this for compare-and-swap semantics
        return ImmutableMap.<String, String>builder()
                .put("expected_parameter_key", "metadata_location")
                .put("expected_parameter_value", metadataLocation)
                .buildOrThrow();
    }
}
