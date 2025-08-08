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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.functions.io.functions.Load.LoadTableHandle;
import io.trino.filesystem.Location;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePageSourceFactory;
import io.trino.plugin.hive.HiveSplit;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.HiveUtil.getInputFormatName;

public class StoragePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final Set<HivePageSourceFactory> pageSourceFactories;

    @Inject
    public StoragePageSourceProvider(Set<HivePageSourceFactory> pageSourceFactories)
    {
        this.pageSourceFactories = ImmutableSet.copyOf(pageSourceFactories);
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        if (table instanceof LoadTableHandle) {
            HiveSplit hiveSplit = (HiveSplit) split;
            Location location = Location.of(hiveSplit.getPath());
            for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
                Optional<ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                        session,
                        location,
                        hiveSplit.getStart(),
                        hiveSplit.getLength(),
                        hiveSplit.getEstimatedFileSize(),
                        hiveSplit.getFileModifiedTime(),
                        hiveSplit.getSchema(),
                        columns.stream().map(HiveColumnHandle.class::cast).collect(toImmutableList()),
                        TupleDomain.all(),
                        hiveSplit.getAcidInfo(),
                        OptionalInt.empty(),
                        true,
                        NO_ACID_TRANSACTION);

                if (pageSource.isPresent()) {
                    return pageSource.get();
                }
            }
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Unsupported input format: serde=%s, format=%s, partition=%s, path=%s".formatted(
                    hiveSplit.getSchema().serializationLibraryName(),
                    getInputFormatName(hiveSplit.getSchema().serdeProperties()).orElse(null),
                    hiveSplit.getPartitionName(),
                    hiveSplit.getPath()));
        }
        throw new IllegalArgumentException("Unsupported table handle: " + table);
    }
}
