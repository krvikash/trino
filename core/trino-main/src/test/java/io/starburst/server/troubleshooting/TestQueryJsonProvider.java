/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.troubleshooting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.stats.Distribution;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.starburst.server.troubleshooting.providers.QueryJsonProvider;
import io.trino.client.NodeVersion;
import io.trino.connector.CatalogHandle;
import io.trino.cost.StatsAndCosts;
import io.trino.execution.QueryInfo;
import io.trino.execution.QueryStats;
import io.trino.execution.StageId;
import io.trino.execution.StageInfo;
import io.trino.execution.StageState;
import io.trino.execution.StageStats;
import io.trino.execution.StagesInfo;
import io.trino.metadata.TableHandle;
import io.trino.operator.BlockedReason;
import io.trino.operator.RetryPolicy;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.tpch.TpchPartitioningHandle;
import io.trino.server.DynamicFilterService;
import io.trino.spi.QueryId;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.catalog.CatalogProperties;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.eventlistener.StageGcStatistics;
import io.trino.spi.metrics.Metrics;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.PlanFragment;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.MergeProcessorNode;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TableWriterNode;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.QueryState.FINISHED;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryJsonProvider
{
    @Test
    public void testQueryJsonDoesntContainCatalogProperties()
            throws IOException
    {
        QueryStats queryStats = new QueryStats(
                ZonedDateTime.of(1991, 9, 6, 5, 0, 0, 0, ZoneOffset.ofHoursMinutes(-5, -30)).toInstant(),
                ZonedDateTime.of(1991, 9, 6, 5, 1, 0, 0, ZoneOffset.ofHoursMinutes(-5, -30)).toInstant(),
                ZonedDateTime.of(1991, 9, 6, 5, 2, 0, 0, ZoneOffset.ofHoursMinutes(-5, -30)).toInstant(),
                ZonedDateTime.of(1991, 9, 6, 6, 0, 0, 0, ZoneOffset.ofHoursMinutes(-5, -30)).toInstant(),
                new Duration(8, MINUTES),
                new Duration(7, MINUTES),
                new Duration(35, MINUTES),
                new Duration(35, MINUTES),
                new Duration(40, MINUTES),
                new Duration(44, MINUTES),
                new Duration(9, MINUTES),
                new Duration(99, SECONDS),
                new Duration(9, SECONDS),
                new Duration(12, MINUTES),
                13,
                14,
                15,
                16,
                161,
                17,
                18,
                34,
                19,
                20.0,
                20.1,
                DataSize.valueOf("21GB"),
                DataSize.valueOf("22GB"),
                DataSize.valueOf("23GB"),
                DataSize.valueOf("24GB"),
                DataSize.valueOf("25GB"),
                DataSize.valueOf("26GB"),
                DataSize.valueOf("27GB"),
                DataSize.valueOf("28GB"),
                DataSize.valueOf("29GB"),
                DataSize.valueOf("30GB"),
                true,
                OptionalDouble.of(8.88),
                OptionalDouble.of(0),
                new Duration(23, MINUTES),
                new Duration(231, MINUTES),
                new Duration(24, MINUTES),
                new Duration(241, MINUTES),
                new Duration(26, MINUTES),
                true,
                ImmutableSet.of(BlockedReason.WAITING_FOR_MEMORY),
                DataSize.valueOf("271GB"),
                DataSize.valueOf("2710GB"),
                281,
                2810,
                new Duration(20, MINUTES),
                new Duration(2001, MINUTES),
                DataSize.valueOf("272GB"),
                DataSize.valueOf("27201GB"),
                282,
                28201,
                DataSize.valueOf("29GB"),
                DataSize.valueOf("2901GB"),
                30,
                3001,
                new Duration(4, MINUTES),
                new Duration(6, MINUTES),
                DataSize.valueOf("31GB"),
                DataSize.valueOf("3101GB"),
                32,
                3201,
                new Duration(3, MINUTES),
                new Duration(5, MINUTES),
                DataSize.valueOf("32GB"),
                DataSize.valueOf("3201GB"),
                ImmutableList.of(new StageGcStatistics(
                        101,
                        102,
                        103,
                        104,
                        105,
                        106,
                        107)),
                DynamicFilterService.DynamicFiltersStats.EMPTY,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableList.of());

        ConnectorPartitioningHandle connectorPartitioningHandle = new TpchPartitioningHandle("t", 1L);
        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.empty(),
                Optional.empty(),
                connectorPartitioningHandle);
        CatalogProperties catalogProperties = new CatalogProperties(
                new CatalogName("pruned_catalog"),
                new CatalogVersion("1"),
                new ConnectorName("tpch"),
                ImmutableMap.of("connection-password", "secret"));
        TableHandle tableHandle = new TableHandle(
                CatalogHandle.createRootCatalogHandle(new CatalogName("test"), new CatalogVersion("123")),
                new ConnectorTableHandle()
                {
                    private String credential = "should_not_leak1";

                    public String getCredential()
                    {
                        return credential;
                    }

                    @Override
                    public String toString()
                    {
                        final StringBuilder sb = new StringBuilder("anonymous ConnectorTableHandle{");
                        sb.append(", credential='").append(credential).append('\'');
                        sb.append('}');
                        return sb.toString();
                    }
                },
                new ConnectorTransactionHandle()
                {
                    private String credential = "should_not_leak1";

                    public String getCredential()
                    {
                        return credential;
                    }

                    @Override
                    public String toString()
                    {
                        final StringBuilder sb = new StringBuilder("anonymous ConnectorTableHandle{");
                        sb.append(", credential='").append(credential).append('\'');
                        sb.append('}');
                        return sb.toString();
                    }
                });
        MergeProcessorNode mergeProcessorNode = new MergeProcessorNode(
                new PlanNodeId("1"),
                new ValuesNode(new PlanNodeId("1"), ImmutableList.of(), ImmutableList.of()),
                new TableWriterNode.MergeTarget(
                        tableHandle,
                        Optional.empty(),
                        new SchemaTableName("test", "table"),
                        new TableWriterNode.MergeParadigmAndTypes(Optional.empty(), ImmutableList.of(), ImmutableList.of(), EMPTY_ROW),
                        ImmutableList.of(),
                        ArrayListMultimap.create()),
                new Symbol(EMPTY_ROW, "name"),
                new Symbol(EMPTY_ROW, "name"),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of());
        PlanFragment planFragment = new PlanFragment(
                new PlanFragmentId("1"),
                mergeProcessorNode,
                ImmutableSet.of(),
                new PartitioningHandle(Optional.empty(), Optional.empty(), connectorPartitioningHandle),
                OptionalInt.empty(),
                ImmutableList.of(),
                new PartitioningScheme(
                        Partitioning.create(partitioningHandle, ImmutableList.of()),
                        ImmutableList.of()),
                OptionalInt.of(1),
                new StatsAndCosts(ImmutableMap.of(), ImmutableMap.of()),
                ImmutableList.of(catalogProperties),
                ImmutableMap.of(),
                Optional.empty());
        StageId stageId = new StageId(new QueryId("query"), 1);
        StageStats stageStats = new StageStats(
                Instant.ofEpochMilli(0),
                ImmutableMap.of(new PlanNodeId("1"), new Distribution().snapshot()),
                ImmutableMap.of(new PlanNodeId("2"), new Metrics(ImmutableMap.of("metric", new LongCount(2)))),
                4,
                5,
                6,
                1,
                7,
                8,
                10,
                26,
                11,
                12.0,
                12.1,
                DataSize.ofBytes(13),
                DataSize.ofBytes(14),
                DataSize.ofBytes(15),
                DataSize.ofBytes(16),
                DataSize.ofBytes(17),
                DataSize.ofBytes(18),
                new Duration(15, NANOSECONDS),
                new Duration(16, NANOSECONDS),
                new Duration(18, NANOSECONDS),
                new Duration(181, NANOSECONDS),
                new Duration(182, NANOSECONDS),
                false,
                ImmutableSet.of(),
                DataSize.ofBytes(191),
                DataSize.ofBytes(192),
                201,
                202,
                new Duration(15, NANOSECONDS),
                new Duration(151, NANOSECONDS),
                DataSize.ofBytes(192),
                DataSize.ofBytes(193),
                202,
                203,
                DataSize.ofBytes(21),
                DataSize.ofBytes(2101),
                22,
                2201,
                new Duration(13, NANOSECONDS),
                new Duration(33, NANOSECONDS),
                DataSize.ofBytes(23),
                Optional.empty(),
                DataSize.ofBytes(24),
                DataSize.ofBytes(241),
                25,
                2501,
                Metrics.EMPTY,
                new Duration(21, NANOSECONDS),
                new Duration(33, NANOSECONDS),
                DataSize.ofBytes(26),
                DataSize.ofBytes(2601),
                new StageGcStatistics(
                        101,
                        102,
                        103,
                        104,
                        105,
                        106,
                        107),
                ImmutableList.of());
        StageInfo stageInfo = new StageInfo(
                stageId,
                StageState.FINISHED,
                planFragment,
                false,
                ImmutableList.of(),
                stageStats,
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                null);
        StagesInfo stagesInfo = new StagesInfo(stageId, ImmutableList.of(stageInfo));

        String queryId = "query1kfhaskdjaksjdka";
        QueryId query1 = new QueryId(queryId);
        QueryInfo queryInfo = new QueryInfo(
                query1,
                TEST_SESSION.toSessionRepresentation(),
                FINISHED,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                "SELECT 4",
                Optional.empty(),
                queryStats,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                false,
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "update",
                Optional.of(stagesInfo),
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                false,
                Optional.empty(),
                Optional.empty(),
                RetryPolicy.QUERY,
                false,
                new NodeVersion("1.0"));
        TroubleshootingContext ctx = new TroubleshootingContext(query1, null);
        ctx.set(QueryInfo.class, queryInfo);
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule());
        QueryJsonProvider provider = new QueryJsonProvider(mapper);
        InputStream inputStream = provider.getInputStreams(ctx).get("query.json");
        byte[] bytes = inputStream.readAllBytes();

        assertThat(new String(bytes, StandardCharsets.UTF_8))
                .doesNotContain("connection-password")
                .doesNotContain("secret")
                .contains("pruned_catalog")
                .doesNotContain("should_not_leak1")
                .doesNotContain("should_not_leak2")
                .contains("\"credential\":\"***\"")
                .contains(queryId);
    }
}
