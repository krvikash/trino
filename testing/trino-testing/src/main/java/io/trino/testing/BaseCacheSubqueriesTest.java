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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.opentelemetry.api.trace.Span;
import io.trino.Session;
import io.trino.cache.CacheDataOperator;
import io.trino.cache.CacheMetadata;
import io.trino.cache.LoadCachedDataOperator;
import io.trino.connector.CatalogHandle;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.Split;
import io.trino.metadata.TableHandle;
import io.trino.operator.OperatorStats;
import io.trino.operator.ScanFilterAndProjectOperator;
import io.trino.operator.TableScanOperator;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.QueryId;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.VarcharType;
import io.trino.split.PageSourceManager.PageSourceProviderInstance;
import io.trino.split.PageSourceProvider;
import io.trino.split.SplitSource;
import io.trino.sql.planner.Plan;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SystemSessionProperties.CACHE_AGGREGATIONS_ENABLED;
import static io.trino.SystemSessionProperties.CACHE_DATA_REDUCTION_THRESHOLD;
import static io.trino.SystemSessionProperties.CACHE_PROJECTIONS_ENABLED;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_ROW_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.cache.CacheDriverFactory.getDynamicRowFilteringUnenforcedPredicate;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.predicate.Range.range;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.BROADCAST;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static io.trino.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.trino.testing.QueryAssertions.assertEqualsIgnoreOrder;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TransactionBuilder.transaction;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.abort;

public abstract class BaseCacheSubqueriesTest
        extends AbstractTestQueryFramework
{
    protected static final Set<TpchTable<?>> REQUIRED_TABLES = ImmutableSet.of(NATION, LINE_ITEM, ORDERS, CUSTOMER);
    protected static final Map<String, String> EXTRA_PROPERTIES = ImmutableMap.of(
            "cache.enabled", "true",
            "dynamic-filtering.bloom-filter.max-distinct-values-per-driver", "1000",
            "dynamic-filtering.partitioned-bloom-filter.max-distinct-values-per-driver", "100",
            "dynamic-filtering.max-distinct-values-per-driver", "100",
            "dynamic-filtering.max-size-per-driver", "100kB",
            "dynamic-filtering.partitioned.max-distinct-values-per-driver", "100",
            "dynamic-filtering.partitioned.max-size-per-driver", "50kB");

    @BeforeEach
    public void flushCache()
    {
        getDistributedQueryRunner().getServers().forEach(server -> server.getCacheManagerRegistry().flushCache());
    }

    public static Object[][] isDynamicRowFilteringEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test
    public void testShowStats()
    {
        assertThat(query("SHOW STATS FOR nation"))
                .result()
                // Not testing average length and min/max, as this would make the test less reusable and is not that important to test.
                .exceptColumns("data_size", "low_value", "high_value")
                .skippingTypesCheck()
                .matches("VALUES " +
                        "('nationkey', 25e0, 0e0, null)," +
                        "('name', 25e0, 0e0, null)," +
                        "('regionkey', 5e0, 0e0, null)," +
                        "('comment', 25e0, 0e0, null)," +
                        "(null, null, null, 25e0)");
    }

    @Test
    public void testUnionWithJoinQuery()
    {
        @Language("SQL") String selectQuery = """
                select c.custkey from (
                  select custkey, nationkey from (select c.custkey, c.nationkey from customer c, nation n where c.nationkey = n.nationkey)
                  union all
                  select custkey, nationkey from (select c.custkey, c.nationkey from customer c, nation n where c.nationkey = n.nationkey)) c
                join nation n on c.nationkey = n.nationkey
                """;
        MaterializedResultWithPlan resultWithCache = executeWithPlan(withBroadcastJoin(withCacheEnabled()), selectQuery);
        MaterializedResultWithPlan resultWithoutCache = executeWithPlan(withBroadcastJoin(withCacheDisabled()), selectQuery);
        assertEqualsIgnoreOrder(resultWithCache.result(), resultWithoutCache.result());
        // make sure data was cached and query succeeds
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.queryId())).isPositive();

        // make sure plan runs local UNION ALL source stage (no repartition remote exchanges)
        Plan plan = getDistributedQueryRunner().getQueryPlan(resultWithCache.queryId());
        int actualRemoteExchangesCount = searchFrom(plan.getRoot())
                .where(node -> node instanceof ExchangeNode exchangeNode
                        && exchangeNode.getScope() == REMOTE
                        // exchanges for distributing nation build tables
                        && exchangeNode.getType() != REPLICATE)
                .findAll()
                .size();
        assertThat(actualRemoteExchangesCount).isEqualTo(0);
    }

    @Test
    public void testSubsequentQueryReadsFromCache()
    {
        // use nation (small table) to minimize memory pressure on CI
        @Language("SQL") String selectQuery = "select nationkey from nation";
        MaterializedResultWithPlan resultWithCache = executeWithPlan(withCacheEnabled(), selectQuery);

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.queryId())).isPositive();

        resultWithCache = executeWithPlan(withCacheEnabled(), selectQuery);
        // make sure data was read from cache as data should be cached across queries
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(resultWithCache.queryId())).isZero();
    }

    @Test
    public void testSubsequentQueryReadsFromCacheWithPredicateOnDataColumn()
    {
        if (!supportsDataColumnPruning()) {
            abort("Data column pruning is not supported");
        }

        MaterializedResultWithPlan resultWithCache = executeWithPlan(
                withCacheEnabled(),
                "SELECT partkey FROM lineitem WHERE orderkey BETWEEN 0 AND 1000000000");

        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.queryId())).isPositive();

        resultWithCache = executeWithPlan(
                withCacheEnabled(),
                "SELECT partkey FROM lineitem WHERE orderkey BETWEEN 0 AND 1000000001");
        // make sure data was read from cache because both "orderkey BETWEEN 0 AND 1000000000"
        // and "orderkey BETWEEN 0 AND 1000000001" should evaluate to TRUE for lineitem splits
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(resultWithCache.queryId())).isZero();

        // query with predicate that doesn't evaluate to TRUE for lineitem splits shouldn't read from cache
        resultWithCache = executeWithPlan(
                withCacheEnabled(),
                "SELECT partkey FROM lineitem WHERE orderkey BETWEEN 0 AND 1000");
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isZero();
    }

    @Test
    public void testSubsequentQueryReadsFromCacheWithDynamicFilterOnDataColumn()
    {
        if (!supportsDataColumnPruning()) {
            abort("Data column pruning is not supported");
        }

        MaterializedResultWithPlan resultWithCache = executeWithPlan(
                withCacheEnabled(),
                """
                        SELECT partkey FROM lineitem l JOIN
                         (SELECT suppkey, orderkey FROM (VALUES (2, 17125), (3, 60000), (4, 60000)) t(suppkey, orderkey)) o
                        ON l.suppkey = o.suppkey AND l.orderkey <= o.orderkey
                        """);
        // make sure data was cached
        assertThat(getCacheDataOperatorInputPositions(resultWithCache.queryId())).isPositive();
        assertThat(getScanSplitsWithDynamicFiltersApplied(resultWithCache.queryId())).isPositive();

        resultWithCache = executeWithPlan(
                withCacheEnabled(),
                """
                        SELECT partkey FROM lineitem l JOIN
                         (SELECT suppkey, orderkey FROM (VALUES (2, 17125), (3, 60000), (4, 60001)) t(suppkey, orderkey)) o
                        ON l.suppkey = o.suppkey AND l.orderkey <= o.orderkey
                        """);
        // make sure data was read from cache because dynamic filters for "l.orderkey < o.orderkey"
        // should evaluate to TRUE for both queries since the highest lineitem "orderkey" value is 60000
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(resultWithCache.queryId())).isZero();

        // query with dynamic filter that doesn't evaluate to TRUE for lineitem splits shouldn't read from cache
        resultWithCache = executeWithPlan(
                withCacheEnabled(),
                """
                        SELECT partkey FROM lineitem l JOIN
                         (SELECT suppkey, orderkey FROM (VALUES (2, 17125), (3, 59999), (4, 59999)) t(suppkey, orderkey)) o
                        ON l.suppkey = o.suppkey AND l.orderkey <= o.orderkey
                        """);
        assertThat(getLoadCachedDataOperatorInputPositions(resultWithCache.queryId())).isZero();
        assertThat(getScanSplitsWithDynamicFiltersApplied(resultWithCache.queryId())).isPositive();
    }

    @ParameterizedTest
    @MethodSource("isDynamicRowFilteringEnabled")
    public void testDynamicFilterCache(boolean isDynamicRowFilteringEnabled)
    {
        String tableName = "orders_part" + randomNameSuffix();
        createPartitionedTableAsSelect(tableName, ImmutableList.of("custkey"), "select orderkey, orderdate, orderpriority, mod(custkey, 10) as custkey from orders");
        @Language("SQL") String totalScanOrdersQuery = "select count(orderkey) from " + tableName;
        @Language("SQL") String firstJoinQuery = """
                select count(orderkey) from %1$s o join (select * from (values 0, 1, 2) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from %1$s o join (select * from (values 0, 1, 2) t(custkey)) t on o.custkey = t.custkey
                """.formatted(tableName);
        @Language("SQL") String secondJoinQuery = """
                select count(orderkey) from %1$s o join (select * from (values 0, 1, 2, 4) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from %1$s o join (select * from (values 0, 1, 2, 3) t(custkey)) t on o.custkey = t.custkey
                """.formatted(tableName);
        @Language("SQL") String thirdJoinQuery = """
                select count(orderkey) from %1$s o join (select * from (values 0, 1) t(custkey)) t on o.custkey = t.custkey
                union all
                select count(orderkey) from %1$s o join (select * from (values 0, 1) t(custkey)) t on o.custkey = t.custkey
                """.formatted(tableName);

        Session cacheSubqueriesEnabled = withDynamicRowFiltering(withCacheEnabled(), isDynamicRowFilteringEnabled);
        Session cacheSubqueriesDisabled = withDynamicRowFiltering(withCacheDisabled(), isDynamicRowFilteringEnabled);
        MaterializedResultWithPlan totalScanOrdersExecution = executeWithPlan(cacheSubqueriesDisabled, totalScanOrdersQuery);
        MaterializedResultWithPlan firstJoinExecution = executeWithPlan(cacheSubqueriesEnabled, firstJoinQuery);
        MaterializedResultWithPlan anotherFirstJoinExecution = executeWithPlan(cacheSubqueriesEnabled, firstJoinQuery);
        MaterializedResultWithPlan secondJoinExecution = executeWithPlan(cacheSubqueriesEnabled, secondJoinQuery);
        MaterializedResultWithPlan thirdJoinExecution = executeWithPlan(cacheSubqueriesEnabled, thirdJoinQuery);

        // firstJoinQuery does not read whole probe side as some splits were pruned by dynamic filters
        assertThat(getScanOperatorInputPositions(firstJoinExecution.queryId())).isLessThan(getScanOperatorInputPositions(totalScanOrdersExecution.queryId()));
        assertThat(getCacheDataOperatorInputPositions(firstJoinExecution.queryId())).isPositive();
        // firstJoinQuery reads from table
        assertThat(getScanOperatorInputPositions(firstJoinExecution.queryId())).isPositive();
        // second run of firstJoinQuery reads only from cache
        assertThat(getScanOperatorInputPositions(anotherFirstJoinExecution.queryId())).isZero();
        assertThat(getLoadCachedDataOperatorInputPositions(anotherFirstJoinExecution.queryId())).isPositive();

        // secondJoinQuery reads from table and cache because its predicate is wider that firstJoinQuery's predicate
        assertThat(getCacheDataOperatorInputPositions(secondJoinExecution.queryId())).isPositive();
        assertThat(getLoadCachedDataOperatorInputPositions(secondJoinExecution.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(secondJoinExecution.queryId())).isPositive();

        // thirdJoinQuery reads only from cache
        assertThat(getLoadCachedDataOperatorInputPositions(thirdJoinExecution.queryId())).isPositive();
        assertThat(getScanOperatorInputPositions(thirdJoinExecution.queryId())).isZero();

        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testConjunctionOfNonDeterministicPredicateAndDynamicFilter()
    {
        MaterializedResultWithPlan resultWithCache = executeWithPlan(
                withCacheEnabled(),
                """
                        SELECT l.partkey
                        FROM
                            (SELECT * FROM orders WHERE random(shippriority + 1) > 20) o
                        JOIN
                            (SELECT * FROM lineitem WHERE random(CAST(quantity AS INTEGER)) > 5) l
                        ON
                            l.ORDERKEY = o.ORDERKEY
                        """);

        // make sure only one side was cached (the one without dynamic filter)
        assertThat(getOperatorStats(resultWithCache.queryId(), CacheDataOperator.class.getSimpleName()).count()).isEqualTo(1L);
    }

    @Test
    public void testPredicateOnPartitioningColumnThatWasNotFullyPushed()
    {
        String tableName = "orders_part" + randomNameSuffix();
        createPartitionedTableAsSelect(tableName, ImmutableList.of("orderkey"), "select orderdate, orderpriority, mod(orderkey, 50) as orderkey from orders");
        // mod predicate will be not pushed to connector
        @Language("SQL") String query =
                """
                        select * from (
                            select orderdate from %1$s where orderkey > 5 and mod(orderkey, 10) = 0 and orderpriority = '1-MEDIUM'
                            union all
                            select orderdate from %1$s where orderkey > 10 and mod(orderkey, 10) = 1 and orderpriority = '3-MEDIUM'
                        ) order by orderdate
                        """.formatted(tableName);
        MaterializedResultWithPlan cacheDisabledResult = executeWithPlan(withCacheDisabled(), query);
        executeWithPlan(withCacheEnabled(), query);
        MaterializedResultWithPlan cacheEnabledResult = executeWithPlan(withCacheEnabled(), query);

        assertThat(getLoadCachedDataOperatorInputPositions(cacheEnabledResult.queryId())).isPositive();
        assertThat(cacheDisabledResult.result()).isEqualTo(cacheEnabledResult.result());
        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testCacheWhenProjectionsWerePushedDown()
    {
        computeActual("create table orders_with_row (c row(name varchar, lastname varchar, age integer))");
        computeActual("insert into orders_with_row values (row (row ('any_name', 'any_lastname', 25)))");

        @Language("SQL") String query = "select c.name, c.age from orders_with_row union all select c.name, c.age from orders_with_row";
        @Language("SQL") String secondQuery = "select c.lastname, c.age from orders_with_row union all select c.lastname, c.age from orders_with_row";

        Session cacheEnabledProjectionDisabled = withProjectionPushdownEnabled(withCacheEnabled(), false);

        MaterializedResultWithPlan firstRun = executeWithPlan(withCacheEnabled(), query);
        assertThat(firstRun.result().getRowCount()).isEqualTo(2);
        assertThat(firstRun.result().getMaterializedRows().get(0).getFieldCount()).isEqualTo(2);
        assertThat(getCacheDataOperatorInputPositions(firstRun.queryId())).isPositive();

        // should use cache
        MaterializedResultWithPlan secondRun = executeWithPlan(withCacheEnabled(), query);
        assertThat(secondRun.result().getRowCount()).isEqualTo(2);
        assertThat(secondRun.result().getMaterializedRows().get(0).getFieldCount()).isEqualTo(2);
        assertThat(getLoadCachedDataOperatorInputPositions(secondRun.queryId())).isPositive();

        // shouldn't use cache because selected cacheColumnIds were different in the first case as projections were pushed down
        MaterializedResultWithPlan pushDownProjectionDisabledRun = executeWithPlan(cacheEnabledProjectionDisabled, query);
        assertThat(pushDownProjectionDisabledRun.result()).isEqualTo(firstRun.result());

        // shouldn't use cache because selected columns are different
        MaterializedResultWithPlan thirdRun = executeWithPlan(withCacheEnabled(), secondQuery);
        assertThat(getLoadCachedDataOperatorInputPositions(thirdRun.queryId())).isLessThanOrEqualTo(1);

        assertUpdate("drop table orders_with_row");
    }

    @ParameterizedTest
    @MethodSource("isDynamicRowFilteringEnabled")
    public void testGetUnenforcedPredicateAndPrunePredicate(boolean isDynamicRowFilteringEnabled)
    {
        String tableName = "get_unenforced_predicate_is_prune_and_prune_orders_part_" + isDynamicRowFilteringEnabled;
        createPartitionedTableAsSelect(tableName, ImmutableList.of("orderpriority"), "select orderkey, orderdate, '9876' as orderpriority from orders");
        DistributedQueryRunner runner = getDistributedQueryRunner();
        Session session = withDynamicRowFiltering(
                Session.builder(getSession())
                        .setQueryId(new QueryId("prune_predicate_" + isDynamicRowFilteringEnabled))
                        .build(),
                isDynamicRowFilteringEnabled);
        transaction(runner.getTransactionManager(), runner.getPlannerContext().getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(session, transactionSession -> {
                    TestingTrinoServer coordinator = runner.getCoordinator();
                    TestingTrinoServer worker = runner.getServers().get(1);
                    checkState(!worker.isCoordinator());
                    String catalog = transactionSession.getCatalog().orElseThrow();
                    CatalogHandle catalogHandle = coordinator.getCatalogHandle(catalog);
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    coordinator.getPlannerContext().getMetadata().getCatalogHandle(transactionSession, catalog);
                    ConnectorTransactionHandle catalogTransaction = coordinator.getTransactionManager().getConnectorTransaction(transactionSession.getTransactionId().orElseThrow(), catalogHandle);
                    Metadata metadata = coordinator.getPlannerContext().getMetadata();
                    TableHandle handle = metadata.getTableHandle(
                            transactionSession,
                            new QualifiedObjectName(catalog, transactionSession.getSchema().orElseThrow(), tableName)).orElseThrow();
                    // Mimic the optimizer's projection pushdown so that connector-specific table handle state
                    // (e.g. IcebergTableHandle#projectedColumns) reflects what would be observed at split generation
                    // time in a real query.
                    handle = applyIdentityProjection(metadata, transactionSession, handle);
                    ConnectorTableHandle connectorTableHandle = handle.connectorHandle();

                    SplitSource splitSource = coordinator.getSplitManager().getSplits(transactionSession, Span.current(), handle, DynamicFilter.EMPTY, alwaysTrue());
                    ConnectorSplit split = getFutureValue(splitSource.getNextBatch(1000)).getSplits().get(0).getConnectorSplit();

                    ColumnHandle partitionColumn = metadata.getColumnHandles(transactionSession, handle).get("orderpriority");
                    assertThat(partitionColumn).isNotNull();
                    ColumnHandle dataColumn = metadata.getColumnHandles(transactionSession, handle).get("orderkey");
                    assertThat(dataColumn).isNotNull();

                    ConnectorPageSourceProvider pageSourceProvider = worker.getConnector(catalogHandle).getPageSourceProviderFactory().createPageSourceProvider();
                    VarcharType type = VarcharType.createVarcharType(4);

                    // getUnenforcedPredicate and prunePredicate should return none if predicate is exclusive on partition column
                    ConnectorSession connectorSession = transactionSession.toConnectorSession(metadata.getCatalogHandle(transactionSession, catalog).orElseThrow());
                    Domain nonPartitionDomain = Domain.multipleValues(type, Streams.concat(LongStream.range(0, 9_000), LongStream.of(9_999))
                            .boxed()
                            .map(value -> Slices.utf8Slice(value.toString()))
                            .collect(toImmutableList()));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split,
                            connectorTableHandle,
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, nonPartitionDomain))))
                            .matches(TupleDomain::isNone);
                    assertThat(getUnenforcedPredicate(
                            new PageSourceProviderInstance(pageSourceProvider),
                            isDynamicRowFilteringEnabled,
                            session,
                            new Split(catalogHandle, split),
                            new TableHandle(catalogHandle, connectorTableHandle, catalogTransaction),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, nonPartitionDomain))))
                            .matches(TupleDomain::isNone);

                    // getUnenforcedPredicate and prunePredicate should prune prefilled column that matches given predicate fully
                    Domain partitionDomain = Domain.singleValue(type, Slices.utf8Slice("9876"));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split,
                            connectorTableHandle,
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, partitionDomain))))
                            .matches(TupleDomain::isAll);
                    assertThat(getUnenforcedPredicate(
                            new PageSourceProviderInstance(pageSourceProvider),
                            isDynamicRowFilteringEnabled,
                            session,
                            new Split(catalogHandle, split),
                            new TableHandle(catalogHandle, connectorTableHandle, catalogTransaction),
                            TupleDomain.withColumnDomains(ImmutableMap.of(partitionColumn, partitionDomain))))
                            .matches(TupleDomain::isAll);

                    // prunePredicate should not prune or simplify data column if there was no predicate on data column
                    Domain dataDomain = Domain.multipleValues(BIGINT, LongStream.range(0, 10_000)
                            .boxed()
                            .collect(toImmutableList()));
                    assertThat(pageSourceProvider.prunePredicate(
                            connectorSession,
                            split,
                            connectorTableHandle,
                            TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                            .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain)));

                    if (supportsDataColumnPruning()) {
                        SplitSource splitSourceWithDfOnDataColumn = coordinator.getSplitManager().getSplits(
                                transactionSession,
                                Span.current(),
                                handle,
                                getDynamicFilter(TupleDomain.withColumnDomains(ImmutableMap.of(
                                        dataColumn,
                                        Domain.create(ValueSet.ofRanges(Range.lessThan(BIGINT, 1_000_000L)), false)))),
                                alwaysTrue());
                        ConnectorSplit splitWithDfOnDataColumn = getFutureValue(splitSourceWithDfOnDataColumn.getNextBatch(1000)).getSplits().get(0).getConnectorSplit();
                        // getUnenforcedPredicate and prunePredicate should prune data column if there is dynamic filter on that column
                        Domain containingRange = Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 60_000L)), false);
                        assertThat(pageSourceProvider.getUnenforcedPredicate(
                                connectorSession,
                                splitWithDfOnDataColumn,
                                connectorTableHandle,
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, containingRange))))
                                .isEqualTo(TupleDomain.all());
                        assertThat(pageSourceProvider.prunePredicate(
                                connectorSession,
                                splitWithDfOnDataColumn,
                                connectorTableHandle,
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, containingRange))))
                                .isEqualTo(TupleDomain.all());
                    }

                    if (isDynamicRowFilteringEnabled || getUnenforcedPredicateIsPrune()) {
                        // getUnenforcedPredicate should not prune or simplify data column
                        assertThat(getUnenforcedPredicate(
                                new PageSourceProviderInstance(pageSourceProvider),
                                isDynamicRowFilteringEnabled,
                                session,
                                new Split(catalogHandle, split),
                                new TableHandle(catalogHandle, connectorTableHandle, catalogTransaction),
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain)));
                    }
                    else {
                        // getUnenforcedPredicate should not prune but simplify data column
                        assertThat(pageSourceProvider.getUnenforcedPredicate(
                                connectorSession,
                                split,
                                connectorTableHandle,
                                TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, dataDomain))))
                                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(dataColumn, Domain.create(ValueSet.ofRanges(range(BIGINT, 0L, true, 9_999L, true)), false))));
                    }
                });
        assertUpdate("drop table " + tableName);
    }

    @Test
    public void testEffectivePredicateReturnedPerSplit()
    {
        if (!effectivePredicateReturnedPerSplit()) {
            abort("Effective predicate is not returned per split");
        }

        DistributedQueryRunner runner = getDistributedQueryRunner();
        transaction(runner.getTransactionManager(), runner.getPlannerContext().getMetadata(), runner.getAccessControl())
                .singleStatement()
                .execute(getSession(), transactionSession -> {
                    TestingTrinoServer coordinator = runner.getCoordinator();
                    TestingTrinoServer worker = runner.getServers().get(1);
                    checkState(!worker.isCoordinator());
                    String catalog = transactionSession.getCatalog().orElseThrow();
                    String schema = transactionSession.getSchema().orElseThrow();
                    Metadata metadata = coordinator.getPlannerContext().getMetadata();
                    TableHandle handle = metadata.getTableHandle(
                            transactionSession,
                            new QualifiedObjectName(catalog, schema, "lineitem")).orElseThrow();
                    // Mimic the optimizer's projection pushdown so that connector-specific table handle state
                    // (e.g. IcebergTableHandle#projectedColumns) reflects what would be observed at split generation
                    // time in a real query.
                    handle = applyIdentityProjection(metadata, transactionSession, handle);
                    ConnectorTableHandle connectorTableHandle = handle.connectorHandle();
                    ColumnHandle orderKeyColumn = metadata.getColumnHandles(transactionSession, handle).get("orderkey");

                    // get table handle with filter applied
                    TupleDomain<ColumnHandle> effectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                            orderKeyColumn, Domain.singleValue(BIGINT, 17125L)));
                    Optional<ConstraintApplicationResult<TableHandle>> filterResult = metadata.applyFilter(
                            transactionSession,
                            handle,
                            new Constraint(effectivePredicate));
                    assertThat(filterResult).isPresent();
                    TableHandle handleWithFilter = filterResult.get().getAlternatives().get(0).handle();
                    ConnectorTableHandle connectorTableHandleWithFilter = handleWithFilter.connectorHandle();

                    // make sure cache table ids are same for both table handles
                    CacheMetadata cacheMetadata = runner.getCacheMetadata();
                    assertThat(cacheMetadata.getCacheTableId(transactionSession, handle)).isEqualTo(cacheMetadata.getCacheTableId(transactionSession, handleWithFilter));

                    // make sure effective predicate is propagated as part of split id
                    SplitSource splitSource = coordinator.getSplitManager().getSplits(transactionSession, Span.current(), handle, DynamicFilter.EMPTY, alwaysTrue());
                    ConnectorSplit split = getFutureValue(splitSource.getNextBatch(1000)).getSplits().get(0).getConnectorSplit();

                    SplitSource splitSourceWithFilter = coordinator.getSplitManager().getSplits(transactionSession, Span.current(), handleWithFilter, DynamicFilter.EMPTY, alwaysTrue());
                    ConnectorSplit splitWithFilter = getFutureValue(splitSourceWithFilter.getNextBatch(1000)).getSplits().get(0).getConnectorSplit();

                    ConnectorPageSourceProvider pageSourceProvider = getPageSourceProvider(worker.getConnector(coordinator.getCatalogHandle(catalog)));
                    ConnectorSession connectorSession = transactionSession.toConnectorSession(metadata.getCatalogHandle(transactionSession, catalog).orElseThrow());

                    // split for original table handle doesn't propagate any effective predicate
                    assertThat(pageSourceProvider.getUnenforcedPredicate(connectorSession, split, connectorTableHandle, TupleDomain.all()))
                            .isEqualTo(TupleDomain.all());
                    // split for filtered table handle should propagate effective predicate
                    assertThat(pageSourceProvider.getUnenforcedPredicate(connectorSession, splitWithFilter, connectorTableHandleWithFilter, TupleDomain.all()))
                            .isEqualTo(effectivePredicate);

                    if (supportsDataColumnPruning()) {
                        // make sure prunePredicate removes predicates that evaluate to ALL for a split
                        assertThat(pageSourceProvider.prunePredicate(
                                connectorSession,
                                splitWithFilter,
                                connectorTableHandleWithFilter,
                                TupleDomain.withColumnDomains(ImmutableMap.of(
                                        orderKeyColumn, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 60_000L)), false)))))
                                .isEqualTo(TupleDomain.all());
                    }
                });
    }

    private ConnectorPageSourceProvider getPageSourceProvider(Connector workerConnector)
    {
        ConnectorPageSourceProvider pageSourceProvider = null;
        try {
            pageSourceProvider = workerConnector.getPageSourceProviderFactory().createPageSourceProvider();
        }
        catch (UnsupportedOperationException ignored) {
        }
        requireNonNull(pageSourceProvider, format("Connector '%s' returned a null page source provider", workerConnector));
        return pageSourceProvider;
    }

    private TupleDomain<ColumnHandle> getUnenforcedPredicate(
            PageSourceProvider pageSourceProvider,
            boolean isDynamicRowFilteringEnabled,
            Session session,
            Split split,
            TableHandle table,
            TupleDomain<ColumnHandle> predicate)
    {
        if (isDynamicRowFilteringEnabled) {
            return getDynamicRowFilteringUnenforcedPredicate(pageSourceProvider, session, split, table, predicate);
        }
        return pageSourceProvider.getUnenforcedPredicate(session, split, table, predicate);
    }

    protected boolean effectivePredicateReturnedPerSplit()
    {
        return true;
    }

    protected boolean supportsDataColumnPruning()
    {
        return true;
    }

    protected boolean getUnenforcedPredicateIsPrune()
    {
        return false;
    }

    protected <T> T withTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return newTransaction().execute(getSession(), transactionSessionConsumer);
    }

    protected MaterializedResultWithPlan executeWithPlan(Session session, @Language("SQL") String sql)
    {
        return getDistributedQueryRunner().executeWithPlan(session, sql);
    }

    protected Long getScanSplitsWithDynamicFiltersApplied(QueryId queryId)
    {
        return getOperatorStats(queryId, TableScanOperator.class.getSimpleName(), ScanFilterAndProjectOperator.class.getSimpleName())
                .map(OperatorStats::getDynamicFilterSplitsProcessed)
                .mapToLong(Long::valueOf)
                .sum();
    }

    protected Long getScanOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, TableScanOperator.class.getSimpleName(), ScanFilterAndProjectOperator.class.getSimpleName());
    }

    protected Long getCacheDataOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, CacheDataOperator.class.getSimpleName());
    }

    protected Long getLoadCachedDataOperatorInputPositions(QueryId queryId)
    {
        return getOperatorInputPositions(queryId, LoadCachedDataOperator.class.getSimpleName());
    }

    protected Long getOperatorInputPositions(QueryId queryId, String... operatorType)
    {
        return getOperatorStats(queryId, operatorType)
                .map(OperatorStats::getInputPositions)
                .mapToLong(Long::valueOf)
                .sum();
    }

    protected Stream<OperatorStats> getOperatorStats(QueryId queryId, String... operatorType)
    {
        ImmutableSet<String> operatorTypes = ImmutableSet.copyOf(operatorType);
        return getDistributedQueryRunner().getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats()
                .getOperatorSummaries()
                .stream()
                .filter(summary -> operatorTypes.contains(summary.getOperatorType()));
    }

    protected Session withCacheEnabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(CACHE_AGGREGATIONS_ENABLED, "true")
                .setSystemProperty(CACHE_PROJECTIONS_ENABLED, "true")
                .setSystemProperty(CACHE_DATA_REDUCTION_THRESHOLD, "100")
                .build();
    }

    protected Session withCacheDisabled()
    {
        return Session.builder(getSession())
                .setSystemProperty(CACHE_AGGREGATIONS_ENABLED, "false")
                .setSystemProperty(CACHE_PROJECTIONS_ENABLED, "false")
                .build();
    }

    protected Session withDynamicRowFiltering(Session baseSession, boolean enabled)
    {
        return Session.builder(baseSession)
                .setSystemProperty(ENABLE_DYNAMIC_ROW_FILTERING, String.valueOf(enabled))
                .build();
    }

    protected Session withBroadcastJoin(Session baseSession)
    {
        return Session.builder(baseSession)
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .build();
    }

    private static TableHandle applyIdentityProjection(Metadata metadata, Session session, TableHandle handle)
    {
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, handle);
        List<ConnectorExpression> projections = columnHandles.entrySet().stream()
                .map(entry -> new Variable(
                        entry.getKey(),
                        metadata.getColumnMetadata(session, handle, entry.getValue()).getType()))
                .collect(toImmutableList());
        return metadata.applyProjection(session, handle, projections, columnHandles)
                .map(ProjectionApplicationResult::getHandle)
                .orElse(handle);
    }

    abstract protected void createPartitionedTableAsSelect(String tableName, List<String> partitionColumns, String asSelect);

    protected Session withProjectionPushdownEnabled(Session session, boolean projectionPushdownEnabled)
    {
        return session;
    }

    private static DynamicFilter getDynamicFilter(TupleDomain<ColumnHandle> tupleDomain)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return tupleDomain.getDomains().map(Map::keySet)
                        .orElseGet(ImmutableSet::of);
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return completedFuture(null);
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return tupleDomain;
            }

            @Override
            public OptionalLong getPreferredDynamicFilterTimeout()
            {
                return OptionalLong.of(0);
            }
        };
    }
}
