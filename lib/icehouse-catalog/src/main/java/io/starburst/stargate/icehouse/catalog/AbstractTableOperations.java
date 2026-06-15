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

import io.trino.annotation.NotThreadSafe;
import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

import static java.util.Objects.requireNonNull;

@NotThreadSafe
public abstract class AbstractTableOperations
        extends BaseMetastoreTableOperations
{
    protected final String schemaName;
    protected final String tableName;
    protected final FileIO fileIo;

    protected String currentMetadataLocation;

    protected AbstractTableOperations(
            FileIO fileIo,
            String schemaName,
            String tableName)
    {
        this.fileIo = requireNonNull(fileIo, "fileIo is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    @NotNull
    @Override
    public String tableName()
    {
        return "%s.%s".formatted(schemaName, tableName);
    }

    @Override
    public FileIO io()
    {
        return fileIo;
    }

    @Override
    protected void doRefresh()
    {
        currentMetadataLocation = getMetadataLocation();
        refreshFromMetadataLocation(currentMetadataLocation, 3);
    }

    @Override
    protected void doCommit(@Nullable TableMetadata base, TableMetadata metadata)
    {
        String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
        commitMetadata(metadata, newMetadataLocation, currentMetadataLocation);
    }

    protected abstract String getMetadataLocation();

    /**
     * Persist {@code newMetadata} at {@code newMetadataLocation}. Implementations
     * record {@code previousMetadataLocation} (captured by {@link #doRefresh()}) as
     * the optimistic-concurrency anchor — either as a backend-specific
     * {@code previous_metadata_location} property (Hive/Glue) or a conditional
     * update predicate (CRDB).
     *
     * <p>{@code newMetadataLocation} is passed separately because Iceberg's
     * {@code writeNewMetadata} returns the written path as a String without
     * stamping it into {@code newMetadata.metadataFileLocation()}, which stays
     * null until the file is read back.
     */
    protected abstract void commitMetadata(TableMetadata newMetadata, String newMetadataLocation, @Nullable String previousMetadataLocation);
}
