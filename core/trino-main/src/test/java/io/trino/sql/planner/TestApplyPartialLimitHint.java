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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.planner.assertions.BasePlanTest;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.testing.PlanTester;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.anyTree;
import static io.trino.sql.planner.assertions.PlanMatchPattern.exchange;
import static io.trino.sql.planner.assertions.PlanMatchPattern.limit;
import static io.trino.sql.planner.assertions.PlanMatchPattern.node;
import static io.trino.sql.planner.assertions.PlanMatchPattern.output;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.trino.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.trino.sql.planner.plan.ExchangeNode.Type.GATHER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_NAME;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

final class TestApplyPartialLimitHint
        extends BasePlanTest
{
    private static final String TEST_SCHEMA = "test_schema";
    private static final String TEST_TABLE = "test_table";

    private static final String COLUMN_A = "column_a";
    private static final String COLUMN_B = "column_b";

    @Override
    protected PlanTester createPlanTester()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(TEST_CATALOG_NAME)
                .setSchema(TEST_SCHEMA)
                .setSystemProperty(TASK_CONCURRENCY, "2"); // force parallel plan even on test nodes with single CPU

        PlanTester planTester = PlanTester.create(sessionBuilder.build());
        planTester.createCatalog(TEST_CATALOG_NAME, createMockFactory(), ImmutableMap.of());
        return planTester;
    }

    @Test
    void testLimitPushdownWithProjection()
    {
        assertDistributedPlan(
                "SELECT column_a * 2 FROM " + TEST_TABLE + " LIMIT 50",
                anyTree(
                        limit(50, ImmutableList.of(), true,
                                tableScan(
                                        tableHandle -> tableHandle instanceof TestingTableHandle testingHandle &&
                                                testingHandle.getLimit().equals(OptionalLong.of(50)),
                                        TupleDomain.all(),
                                        ImmutableMap.of(COLUMN_A, _ -> true)))));
    }

    @Test
    void testLimitPushdownThroughFilter()
    {
        assertDistributedPlan(
                "SELECT column_a FROM " + TEST_TABLE + " WHERE column_b = 'test' LIMIT 25",
                anyTree(
                        limit(25, ImmutableList.of(), true,
                                node(ProjectNode.class,
                                        node(FilterNode.class,
                                                tableScan(
                                                        tableHandle -> tableHandle instanceof TestingTableHandle testingHandle &&
                                                                testingHandle.getLimit().equals(OptionalLong.of(25)),
                                                        TupleDomain.all(),
                                                        ImmutableMap.of()))))));
    }

    @Test
    void testLimitPushdownThroughExchange()
    {
        assertDistributedPlan(
                "SELECT column_a FROM " + TEST_TABLE + " LIMIT 75",
                anyTree(
                        exchange(REMOTE, GATHER,
                                limit(75, ImmutableList.of(), true,
                                        tableScan(
                                                tableHandle -> tableHandle instanceof TestingTableHandle testingHandle &&
                                                        testingHandle.getLimit().equals(OptionalLong.of(75)),
                                                TupleDomain.all(),
                                                ImmutableMap.of("A", _ -> true))))));
    }

    @Test
    void testNestedLimitPushdown()
    {
        assertDistributedPlan(
                "SELECT * FROM (SELECT column_a FROM " + TEST_TABLE + " LIMIT 200) LIMIT 10",
                anyTree(
                        limit(10,
                                anyTree(
                                        tableScan(
                                                tableHandle -> tableHandle instanceof TestingTableHandle testingHandle &&
                                                        testingHandle.getLimit().equals(OptionalLong.of(10)),
                                                TupleDomain.all(),
                                                ImmutableMap.of("A", _ -> true))))));
    }

    @Test
    void testNoLimitPushdownWithJoin()
    {
        assertPlan(
                "SELECT t1.column_a FROM " + TEST_TABLE + " t1 JOIN " + TEST_TABLE + " t2 ON t1.column_a = t2.column_a LIMIT 100",
                anyTree(
                        limit(100,
                                anyTree(
                                        tableScan(
                                                tableHandle -> tableHandle instanceof TestingTableHandle testingHandle &&
                                                        testingHandle.getLimit().isEmpty(),
                                                TupleDomain.all(),
                                                ImmutableMap.of())))));
    }

    @Test
    void testNoLimitPushdownWithAggregation()
    {
        assertPlan(
                "SELECT column_b, COUNT(*) FROM " + TEST_TABLE + " GROUP BY column_b LIMIT 10",
                anyTree(
                        limit(10,
                                anyTree(
                                        tableScan(
                                                tableHandle -> tableHandle instanceof TestingTableHandle testingHandle &&
                                                        testingHandle.getLimit().isEmpty(),
                                                TupleDomain.all(),
                                                ImmutableMap.of())))));
    }

    @Test
    void testNoLimitWithoutLimitNode()
    {
        assertPlan("SELECT column_a FROM " + TEST_TABLE,
                output(
                        tableScan(
                                tableHandle -> tableHandle instanceof TestingTableHandle testingHandle &&
                                        testingHandle.getLimit().isEmpty(),
                                TupleDomain.all(),
                                ImmutableMap.of("A", _ -> true))));
    }

    private MockConnectorFactory createMockFactory()
    {
        return MockConnectorFactory.builder()
                .withGetTableHandle((_, schemaTableName) -> new TestingTableHandle(schemaTableName))
                .withGetColumns(_ -> ImmutableList.of(
                        new ColumnMetadata(COLUMN_A, BIGINT),
                        new ColumnMetadata(COLUMN_B, VARCHAR)))
                .withApplyPartialLimit((_, handle, limitHint) -> {
                    if (handle instanceof TestingTableHandle tableHandleWithLimit) {
                        return Optional.of(tableHandleWithLimit.withLimit(limitHint));
                    }
                    return Optional.empty();
                })
                .build();
    }

    /**
     * Custom table handle that tracks the partial limit hint
     */
    private static class TestingTableHandle
            extends MockConnectorTableHandle
    {
        private final OptionalLong limit;

        public TestingTableHandle(SchemaTableName tableName)
        {
            this(tableName, TupleDomain.all(), Optional.empty(), OptionalLong.empty());
        }

        public TestingTableHandle(
                SchemaTableName tableName,
                TupleDomain<ColumnHandle> constraint,
                Optional<List<ColumnHandle>> columns,
                OptionalLong limit)
        {
            super(tableName, constraint, columns);
            this.limit = requireNonNull(limit, "limit is null");
        }

        public OptionalLong getLimit()
        {
            return limit;
        }

        public TestingTableHandle withLimit(long limit)
        {
            return new TestingTableHandle(getTableName(), getConstraint(), getColumns(), OptionalLong.of(limit));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            if (!super.equals(o)) {
                return false;
            }
            TestingTableHandle that = (TestingTableHandle) o;
            return Objects.equals(limit, that.limit);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), limit);
        }

        @Override
        public String toString()
        {
            return getTableName().toString() + (limit.isPresent() ? " with limit " + limit.getAsLong() : "");
        }
    }
}
