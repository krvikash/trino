/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.saphana;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.license.LicenseManager;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMultiset.toImmutableMultiset;
import static com.starburstdata.trino.plugin.saphana.SapHanaParallelismType.NO_PARALLELISM;
import static com.starburstdata.trino.plugin.saphana.SapHanaParallelismType.PARTITIONS;
import static com.starburstdata.trino.plugin.saphana.SapHanaSessionProperties.getParallelismType;
import static io.trino.plugin.jdbc.DynamicFilteringJdbcSplitSource.isEligibleForDynamicFilter;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SapHanaSplitManager
        implements ConnectorSplitManager
{
    private final ConnectionFactory connectionFactory;

    @Inject
    public SapHanaSplitManager(
            LicenseManager licenseManager,
            ConnectionFactory connectionFactory,
            SapHanaConfig config)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        if (config.getParallelismType() != NO_PARALLELISM) {
            licenseManager.checkLicense();
        }
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        return new FixedSplitSource(listSplits(
                session,
                (JdbcTableHandle) table,
                getParallelismType(session),
                isEligibleForDynamicFilter((JdbcTableHandle) table)
                        ? dynamicFilter.getCurrentPredicate().transformKeys(JdbcColumnHandle.class::cast)
                        : TupleDomain.all()));
    }

    private List<SapHanaSplit> listSplits(
            ConnectorSession session,
            JdbcTableHandle tableHandle,
            SapHanaParallelismType parallelismType,
            TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        if (parallelismType == NO_PARALLELISM || !tableHandle.isNamedRelation()) {
            return ImmutableList.of(new SapHanaSplit(Optional.empty(), Optional.empty(), dynamicFilter));
        }

        if (parallelismType == PARTITIONS) {
            List<Integer> partitionIds = listPartitionIdsForTable(session, tableHandle);

            if (partitionIds.isEmpty()) {
                // Table is not partitioned
                return ImmutableList.of(new SapHanaSplit(Optional.empty(), Optional.empty(), dynamicFilter));
            }

            List<Integer> duplicatedPartitions = getDuplicates(partitionIds);
            verify(duplicatedPartitions.isEmpty(), "Partition ids are not unique for table %s: %s", tableHandle, duplicatedPartitions);

            // Partition partitions into batches to limit total number of splits
            return partitionIds.stream()
                    .map(partitionId -> new SapHanaSplit(Optional.of(partitionId), Optional.empty(), dynamicFilter))
                    .collect(toImmutableList());
        }

        throw new IllegalArgumentException(format("Parallelism type %s is not supported", parallelismType));
    }

    private List<Integer> getDuplicates(List<Integer> values)
    {
        return values.stream()
                .collect(toImmutableMultiset()).entrySet().stream()
                .filter(entry -> entry.getCount() > 1)
                .map(Multiset.Entry::getElement)
                .collect(toImmutableList());
    }

    private List<Integer> listPartitionIdsForTable(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        // https://help.sap.com/docs/hana-cloud-database/sap-hana-cloud-sap-hana-database-sql-reference-guide/table-partitions-system-view
        try (Handle handle = Jdbi.open(() -> connectionFactory.openConnection(session))) {
            RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("SELECT DISTINCT PART_ID FROM SYS.TABLE_PARTITIONS WHERE TABLE_NAME = :table AND SCHEMA_NAME = :schema AND PART_ID IS NOT NULL")
                    .bind("table", remoteTableName.getTableName())
                    .bind("schema", remoteTableName.getSchemaName().orElse(null))
                    .mapTo(Integer.class)
                    .list();
        }
        catch (JdbiException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
