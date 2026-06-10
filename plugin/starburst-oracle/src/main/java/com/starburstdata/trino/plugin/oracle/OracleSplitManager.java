/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multiset;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.license.LicenseVerifier;
import io.airlift.log.Logger;
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
import static com.google.common.math.IntMath.divide;
import static com.starburstdata.trino.plugin.oracle.OracleParallelismType.NO_PARALLELISM;
import static com.starburstdata.trino.plugin.oracle.StarburstOracleSessionProperties.getMaxSplitsPerScan;
import static com.starburstdata.trino.plugin.oracle.StarburstOracleSessionProperties.getParallelismType;
import static io.trino.plugin.jdbc.DynamicFilteringJdbcSplitSource.isEligibleForDynamicFilter;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.math.RoundingMode.CEILING;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Gatherers.windowFixed;

public class OracleSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(OracleSplitManager.class);

    private final ConnectionFactory connectionFactory;

    @Inject
    public OracleSplitManager(
            ConnectionFactory connectionFactory,
            StarburstOracleConfig starburstOracleConfig,
            LicenseVerifier licenseVerifier)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        if (starburstOracleConfig.getParallelismType() != NO_PARALLELISM) {
            licenseVerifier.checkLicense();
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
                getMaxSplitsPerScan(session),
                isEligibleForDynamicFilter((JdbcTableHandle) table)
                        ? dynamicFilter.getCurrentPredicate().transformKeys(JdbcColumnHandle.class::cast)
                        : TupleDomain.all()));
    }

    private List<OracleSplit> listSplits(
            ConnectorSession session,
            JdbcTableHandle tableHandle,
            OracleParallelismType parallelismType,
            int maxSplits,
            TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        if (!tableHandle.isNamedRelation()) {
            return singleSplit(dynamicFilter);
        }

        return switch (parallelismType) {
            case NO_PARALLELISM -> singleSplit(dynamicFilter);
            case PARTITIONS -> listPartitionSplits(session, tableHandle, maxSplits, dynamicFilter)
                    .orElseGet(() -> singleSplit(dynamicFilter));
            case ORA_HASH -> listOraHashSplits(session, tableHandle, maxSplits, dynamicFilter)
                    .orElseGet(() -> singleSplit(dynamicFilter));
            case AUTO -> listPartitionSplits(session, tableHandle, maxSplits, dynamicFilter)
                    .or(() -> listOraHashSplits(session, tableHandle, maxSplits, dynamicFilter))
                    .orElseGet(() -> singleSplit(dynamicFilter));
        };
    }

    private static List<OracleSplit> singleSplit(TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        return ImmutableList.of(new OracleSplit(Optional.empty(), Optional.empty(), dynamicFilter));
    }

    private Optional<List<OracleSplit>> listPartitionSplits(
            ConnectorSession session,
            JdbcTableHandle tableHandle,
            int maxSplits,
            TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        List<String> partitions = listPartitionsForTable(session, tableHandle);
        if (partitions.isEmpty()) {
            return Optional.empty();
        }
        List<String> duplicatedPartitions = getDuplicates(partitions);
        verify(duplicatedPartitions.isEmpty(), "Partition names are not unique: %s", duplicatedPartitions);

        return Optional.of(partitions.stream()
                .gather(windowFixed(divide(partitions.size(), maxSplits, CEILING)))
                .map(batch -> new OracleSplit(Optional.of(batch), Optional.empty(), dynamicFilter))
                .collect(toImmutableList()));
    }

    private List<String> listPartitionsForTable(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try (Handle handle = Jdbi.open(() -> connectionFactory.openConnection(session))) {
            RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
            return handle.createQuery("SELECT partition_name FROM all_tab_partitions WHERE table_name = :name AND table_owner = :owner")
                    .bind("name", remoteTableName.getTableName())
                    .bind("owner", remoteTableName.getSchemaName().orElse(null))
                    .mapTo(String.class)
                    .list();
        }
        catch (JdbiException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private static List<String> getDuplicates(List<String> values)
    {
        return values.stream()
                .collect(toImmutableMultiset()).entrySet().stream()
                .filter(entry -> entry.getCount() > 1)
                .map(Multiset.Entry::getElement)
                .collect(toImmutableList());
    }

    private Optional<List<OracleSplit>> listOraHashSplits(
            ConnectorSession session,
            JdbcTableHandle tableHandle,
            int maxSplits,
            TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();
        String owner = remoteTableName.getSchemaName().orElse(null);
        if (owner == null || !supportsOraHash(session, remoteTableName, owner)) {
            log.warn("ORA_HASH parallel reads are not supported for table %s (external table, view, synonym, or unknown schema); falling back to single split", tableHandle);
            return Optional.empty();
        }

        ImmutableList.Builder<OracleSplit> splits = ImmutableList.builderWithExpectedSize(maxSplits);
        for (int i = 0; i < maxSplits; i++) {
            String predicate = format("ORA_HASH(ROWID, %s, 0) = %s", maxSplits - 1, i);
            splits.add(new OracleSplit(Optional.empty(), Optional.of(predicate), dynamicFilter));
        }
        return Optional.of(splits.build());
    }

    private boolean supportsOraHash(ConnectorSession session, RemoteTableName remoteTableName, String owner)
    {
        try (Handle handle = Jdbi.open(() -> connectionFactory.openConnection(session))) {
            return handle.createQuery("SELECT EXTERNAL FROM ALL_TABLES WHERE TABLE_NAME = :name AND OWNER = :owner")
                    .bind("name", remoteTableName.getTableName())
                    .bind("owner", owner)
                    .mapTo(String.class)
                    .findFirst()
                    .map(external -> !"YES".equals(external))
                    .orElse(false);
        }
        catch (JdbiException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }
}
