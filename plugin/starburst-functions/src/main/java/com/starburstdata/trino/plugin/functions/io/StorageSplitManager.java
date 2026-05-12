/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.io;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.functions.io.functions.Load.LoadTableHandle;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.InternalHiveSplit;
import io.trino.plugin.hive.fs.TrinoFileStatus;
import io.trino.plugin.hive.util.InternalHiveSplitFactory;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;

import java.io.IOException;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.metastore.HivePartition.UNPARTITIONED_ID;
import static io.trino.plugin.hive.HiveMetadata.CSV_ESCAPE_KEY;
import static io.trino.plugin.hive.HiveMetadata.CSV_QUOTE_KEY;
import static io.trino.plugin.hive.HiveMetadata.CSV_SEPARATOR_KEY;
import static io.trino.plugin.hive.HiveMetadata.SKIP_HEADER_COUNT_KEY;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.plugin.hive.util.SerdeConstants.SERIALIZATION_LIB;
import static java.util.Objects.requireNonNull;

public class StorageSplitManager
        implements ConnectorSplitManager
{
    private static final String FILE_INPUT_FORMAT = "file.inputformat";

    private final TrinoFileSystemFactory fileSystemFactory;
    private final DataSize maxInitialSplitSize;
    private final boolean forceLocalScheduling;

    @Inject
    public StorageSplitManager(TrinoFileSystemFactory fileSystemFactory, HiveConfig config)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        maxInitialSplitSize = config.getMaxInitialSplitSize();
        forceLocalScheduling = config.isForceLocalScheduling();
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        if (handle instanceof LoadTableHandle loadTableHandle) {
            HiveStorageFormat format = loadTableHandle.format();
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<String> columnTypes = ImmutableList.builder();
            for (HiveColumnHandle column : loadTableHandle.columns()) {
                columnNames.add(column.getName());
                columnTypes.add(column.getHiveType().getHiveTypeName().toString());
            }
            String columns = String.join(",", columnNames.build());
            String listColumnTypes = String.join(":", columnTypes.build());
            ImmutableMap.Builder<String, String> schemaBuilder = ImmutableMap.<String, String>builder()
                    .put(SERIALIZATION_LIB, format.getSerde())
                    .put(FILE_INPUT_FORMAT, format.getInputFormat())
                    .put(LIST_COLUMNS, columns)
                    .put(LIST_COLUMN_TYPES, listColumnTypes);
            loadTableHandle.skipHeader().ifPresent(number -> schemaBuilder.put(SKIP_HEADER_COUNT_KEY, Integer.toString(number)));
            loadTableHandle.fieldSeparator().ifPresent(separator -> schemaBuilder.put(CSV_SEPARATOR_KEY, String.valueOf(separator)));
            loadTableHandle.quote().ifPresent(quote -> schemaBuilder.put(CSV_QUOTE_KEY, String.valueOf(quote)));
            loadTableHandle.escape().ifPresent(escape -> schemaBuilder.put(CSV_ESCAPE_KEY, String.valueOf(escape)));
            InternalHiveSplitFactory internalSplitFactory = new InternalHiveSplitFactory(
                    UNPARTITIONED_ID,
                    format,
                    schemaBuilder.buildOrThrow(),
                    ImmutableList.of(),
                    TupleDomain.all(),
                    Constraint.alwaysTrue(),
                    () -> true,
                    ImmutableMap.of(),
                    Optional.empty(),
                    Optional.empty(),
                    maxInitialSplitSize,
                    forceLocalScheduling,
                    Optional.empty());

            ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
            try {
                TrinoFileSystem fileSystem = fileSystemFactory.create(session);
                if (loadTableHandle.isDirectory()) {
                    FileIterator files = fileSystem.listFiles(Location.of(loadTableHandle.location()));
                    while (files.hasNext()) {
                        FileEntry file = files.next();
                        String name = file.location().fileName();
                        if (name.startsWith("_") || name.startsWith(".")) {
                            // Skip hidden files
                            continue;
                        }
                        HiveSplit hiveSplit = toHiveSplit(file, internalSplitFactory);
                        splits.add(hiveSplit);
                    }
                }
                else {
                    TrinoInputFile inputFile = fileSystem.newInputFile(Location.of(loadTableHandle.location()));
                    FileEntry fileEntry = new FileEntry(inputFile.location(), inputFile.length(), inputFile.lastModified(), Optional.empty());
                    splits.add(toHiveSplit(fileEntry, internalSplitFactory));
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new FixedSplitSource(splits.build());
        }
        throw new IllegalArgumentException("Unsupported table handle: " + handle);
    }

    private static HiveSplit toHiveSplit(FileEntry entry, InternalHiveSplitFactory internalSplitFactory)
    {
        TrinoFileStatus fileStatus = new TrinoFileStatus(entry);
        InternalHiveSplit internalSplit = internalSplitFactory.createInternalHiveSplit(fileStatus, OptionalInt.empty(), OptionalInt.empty(), false, Optional.empty()).orElseThrow();
        return new HiveSplit(
                internalSplit.getPartitionName(),
                internalSplit.getPath(),
                internalSplit.getStart(),
                internalSplit.getEnd(),
                internalSplit.getEstimatedFileSize(),
                internalSplit.getFileModifiedTime(),
                internalSplit.getSchema(),
                internalSplit.getPartitionKeys(),
                internalSplit.currentBlock().addresses(),
                Optional.empty(),
                internalSplit.getReadBucketNumber(),
                internalSplit.getTableBucketNumber(),
                internalSplit.isForceLocalScheduling(),
                internalSplit.getHiveColumnCoercions(),
                internalSplit.getBucketConversion(),
                internalSplit.getBucketValidation(),
                internalSplit.getAcidInfo(),
                SplitWeight.standard());
    }
}
