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
import io.trino.connector.CatalogHandle;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.ArrayType;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the static eligibility predicate
 * {@link LocalExecutionPlanner#isGpuLocalExchangeEligible}.
 * These tests cover the "cheap gate" rejections without touching the GPU planner path
 * or cuDF native libraries.
 */
public class TestLocalExecutionPlannerGpuLocalExchange
{
    private static final PlanNodeId NODE_ID = new PlanNodeId("exchange-0");
    private static final PlanNodeId SOURCE_ID = new PlanNodeId("values-0");

    // Symbols with GPU-compatible types
    private static final Symbol BIGINT_SYMBOL = new Symbol(BIGINT, "col_bigint");
    private static final Symbol INTEGER_SYMBOL = new Symbol(INTEGER, "col_int");

    // Symbol with a non-GPU-compatible type (ARRAY is not in GpuTypeConversion)
    private static final Symbol ARRAY_SYMBOL = new Symbol(new ArrayType(INTEGER), "col_array");

    @Test
    public void testEligibleForSingleDistribution()
    {
        ExchangeNode node = singleDistributionGather(BIGINT_SYMBOL);
        assertThat(LocalExecutionPlanner.isGpuLocalExchangeEligible(node)).isTrue();
    }

    @Test
    public void testEligibleForFixedHashDistribution()
    {
        ExchangeNode node = fixedHashRepartition(ImmutableList.of(BIGINT_SYMBOL, INTEGER_SYMBOL), BIGINT_SYMBOL);
        assertThat(LocalExecutionPlanner.isGpuLocalExchangeEligible(node)).isTrue();
    }

    @Test
    public void testFallsBackForOrderingScheme()
    {
        Symbol orderSymbol = BIGINT_SYMBOL;
        ValuesNode source = new ValuesNode(SOURCE_ID, ImmutableList.of(orderSymbol));
        // ExchangeNode with an ordering scheme requires FIXED_PASSTHROUGH_DISTRIBUTION for LOCAL scope
        // so we use the static method directly with a mocked ordering-present scenario via REMOTE scope
        // instead. However, the simplest approach is to just check the static predicate cannot accept
        // SINGLE_DISTRIBUTION + ordering: ExchangeNode's constructor enforces FIXED_PASSTHROUGH for
        // local merging exchanges, so we test the predicate by providing a node whose orderingScheme
        // field is present. We build a REMOTE/GATHER node with ordering (which is legal per ExchangeNode
        // constructor) and check our predicate correctly returns false.
        PartitioningScheme scheme = new PartitioningScheme(
                Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                ImmutableList.of(orderSymbol));
        OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(orderSymbol), ImmutableMap.of(orderSymbol, SortOrder.ASC_NULLS_FIRST));
        ExchangeNode node = new ExchangeNode(
                NODE_ID,
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.REMOTE,
                scheme,
                ImmutableList.of(source),
                ImmutableList.of(ImmutableList.of(orderSymbol)),
                Optional.of(orderingScheme));
        assertThat(LocalExecutionPlanner.isGpuLocalExchangeEligible(node)).isFalse();
    }

    @Test
    public void testRoundRobinPartitioningIsEligible()
    {
        ExchangeNode node = arbitraryRepartition(BIGINT_SYMBOL);
        assertThat(LocalExecutionPlanner.isGpuLocalExchangeEligible(node)).isTrue();
    }

    @Test
    public void testFallsBackForConnectorPartitioning()
    {
        // A PartitioningHandle with a catalogHandle present => connector partitioning
        CatalogHandle catalogHandle = CatalogHandle.createRootCatalogHandle(new CatalogName("hive"), new CatalogVersion("1"));
        PartitioningHandle connectorHandle = new PartitioningHandle(
                Optional.of(catalogHandle),
                Optional.of(new ConnectorTransactionHandle() {}),
                SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION.getConnectorHandle());
        PartitioningScheme scheme = new PartitioningScheme(
                Partitioning.create(connectorHandle, ImmutableList.of()),
                ImmutableList.of(BIGINT_SYMBOL));
        ValuesNode source = new ValuesNode(SOURCE_ID, ImmutableList.of(BIGINT_SYMBOL));
        ExchangeNode node = new ExchangeNode(
                NODE_ID,
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.LOCAL,
                scheme,
                ImmutableList.of(source),
                ImmutableList.of(ImmutableList.of(BIGINT_SYMBOL)),
                Optional.empty());
        assertThat(LocalExecutionPlanner.isGpuLocalExchangeEligible(node)).isFalse();
    }

    @Test
    public void testFallsBackForNonConvertibleOutputType()
    {
        // ARRAY(INTEGER) is not convertible
        ExchangeNode node = singleDistributionGather(ARRAY_SYMBOL);
        assertThat(LocalExecutionPlanner.isGpuLocalExchangeEligible(node)).isFalse();
    }

    @Test
    public void testFallsBackForNonConvertiblePartitionChannel()
    {
        // Output has bigint (convertible) but partition key is an array (not convertible)
        ExchangeNode node = fixedHashRepartition(ImmutableList.of(BIGINT_SYMBOL, ARRAY_SYMBOL), ARRAY_SYMBOL);
        assertThat(LocalExecutionPlanner.isGpuLocalExchangeEligible(node)).isFalse();
    }

    // --- helpers ---

    private static ExchangeNode singleDistributionGather(Symbol outputSymbol)
    {
        ValuesNode source = new ValuesNode(SOURCE_ID, ImmutableList.of(outputSymbol));
        PartitioningScheme scheme = new PartitioningScheme(
                Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                ImmutableList.of(outputSymbol));
        return new ExchangeNode(
                NODE_ID,
                ExchangeNode.Type.GATHER,
                ExchangeNode.Scope.LOCAL,
                scheme,
                ImmutableList.of(source),
                ImmutableList.of(ImmutableList.of(outputSymbol)),
                Optional.empty());
    }

    private static ExchangeNode fixedHashRepartition(List<Symbol> outputSymbols, Symbol partitionSymbol)
    {
        ValuesNode source = new ValuesNode(SOURCE_ID, outputSymbols);
        PartitioningScheme scheme = new PartitioningScheme(
                Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(partitionSymbol)),
                ImmutableList.copyOf(outputSymbols));
        return new ExchangeNode(
                NODE_ID,
                ExchangeNode.Type.REPARTITION,
                ExchangeNode.Scope.LOCAL,
                scheme,
                ImmutableList.of(source),
                ImmutableList.of(ImmutableList.copyOf(outputSymbols)),
                Optional.empty());
    }

    private static ExchangeNode arbitraryRepartition(Symbol outputSymbol)
    {
        ValuesNode source = new ValuesNode(SOURCE_ID, ImmutableList.of(outputSymbol));
        PartitioningScheme scheme = new PartitioningScheme(
                Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()),
                ImmutableList.of(outputSymbol));
        return new ExchangeNode(
                NODE_ID,
                ExchangeNode.Type.REPARTITION,
                ExchangeNode.Scope.LOCAL,
                scheme,
                ImmutableList.of(source),
                ImmutableList.of(ImmutableList.of(outputSymbol)),
                Optional.empty());
    }
}
