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
package io.trino.cache;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.cache.CacheController.CacheCandidate;
import io.trino.cache.CanonicalSubplan.AggregationKey;
import io.trino.cache.CanonicalSubplan.ScanFilterProjectKey;
import io.trino.cache.CanonicalSubplan.TopNRankingKey;
import io.trino.metadata.TableHandle;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.subquery.cache.CacheColumnId;
import io.trino.spi.subquery.cache.CacheTableId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.TopNRankingNode.RankingType;
import io.trino.sql.planner.plan.ValuesNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.trino.SystemSessionProperties.SUBQUERY_CACHE_AGGREGATIONS_ENABLED;
import static io.trino.SystemSessionProperties.SUBQUERY_CACHE_PROJECTIONS_ENABLED;
import static io.trino.connector.CatalogHandle.createRootCatalogHandle;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCacheController
{
    private static final PlanNodeId PLAN_NODE_ID = new PlanNodeId("id");
    private static final CacheTableId TABLE_ID = new CacheTableId("table");
    private static final CacheColumnId COLUMN_A = new CacheColumnId("A");
    private static final CacheColumnId COLUMN_B = new CacheColumnId("B");
    public static final TableHandle TABLE_HANDLE = new TableHandle(createRootCatalogHandle(new CatalogName("catalog"), new CatalogVersion("version")), new ConnectorTableHandle() {}, new ConnectorTransactionHandle() {});

    @Test
    public void testCacheController()
    {
        CanonicalSubplan firstGroupByAB = createCanonicalAggregationSubplan(ImmutableSet.of(COLUMN_A, COLUMN_B));
        CanonicalSubplan secondGroupByAB = createCanonicalAggregationSubplan(ImmutableSet.of(COLUMN_A, COLUMN_B));
        CanonicalSubplan groupByA = createCanonicalAggregationSubplan(ImmutableSet.of(COLUMN_A));
        CanonicalSubplan firstProjection = createCanonicalTableScanSubplan();
        CanonicalSubplan secondProjection = createCanonicalTableScanSubplan();
        CanonicalSubplan topN = createCanonicalTopNSubplan(ImmutableMap.of(COLUMN_A, SortOrder.ASC_NULLS_FIRST), 10);
        CanonicalSubplan topNRanking = createCanonicalTopNRankingSubplan(ImmutableList.of(COLUMN_B), ImmutableMap.of(COLUMN_A, SortOrder.ASC_NULLS_FIRST), RankingType.ROW_NUMBER, 10);
        List<CanonicalSubplan> subplans = ImmutableList.of(secondProjection, firstProjection, groupByA, secondGroupByAB, firstGroupByAB);

        CacheController cacheController = new CacheController();
        assertThat(cacheController.getCachingCandidates(cacheProperties(true, true), subplans))
                .containsExactly(
                        // aggregations first
                        new CacheCandidate(ImmutableList.of(groupByA)),
                        new CacheCandidate(ImmutableList.of(secondGroupByAB)),
                        new CacheCandidate(ImmutableList.of(firstGroupByAB)),
                        // then projections
                        new CacheCandidate(ImmutableList.of(secondProjection)),
                        new CacheCandidate(ImmutableList.of(firstProjection)));

        assertThat(cacheController.getCachingCandidates(cacheProperties(true, false), subplans))
                .containsExactly(
                        new CacheCandidate(ImmutableList.of(groupByA)),
                        new CacheCandidate(ImmutableList.of(secondGroupByAB)),
                        new CacheCandidate(ImmutableList.of(firstGroupByAB)));

        assertThat(cacheController.getCachingCandidates(cacheProperties(false, true), subplans))
                .containsExactly(
                        new CacheCandidate(ImmutableList.of(secondProjection)),
                        new CacheCandidate(ImmutableList.of(firstProjection)));

        subplans = ImmutableList.of(secondProjection, firstProjection, topN);
        assertThat(cacheController.getCachingCandidates(cacheProperties(true, true), subplans))
                .containsExactly(
                        // topN (treated as aggregation) first
                        new CacheCandidate(ImmutableList.of(topN)),
                        // then projections
                        new CacheCandidate(ImmutableList.of(secondProjection)),
                        new CacheCandidate(ImmutableList.of(firstProjection)));

        assertThat(cacheController.getCachingCandidates(cacheProperties(true, false), subplans))
                .containsExactly(new CacheCandidate(ImmutableList.of(topN)));
        assertThat(cacheController.getCachingCandidates(cacheProperties(false, true), subplans))
                .containsExactly(
                        new CacheCandidate(ImmutableList.of(secondProjection)),
                        new CacheCandidate(ImmutableList.of(firstProjection)));

        subplans = ImmutableList.of(secondProjection, firstProjection, topNRanking);

        assertThat(cacheController.getCachingCandidates(cacheProperties(true, true), subplans))
                .containsExactly(
                        // topNRanking (treated as aggregation) first
                        new CacheCandidate(ImmutableList.of(topNRanking)),
                        // then projections
                        new CacheCandidate(ImmutableList.of(secondProjection)),
                        new CacheCandidate(ImmutableList.of(firstProjection)));

        assertThat(cacheController.getCachingCandidates(cacheProperties(true, false), subplans))
                .containsExactly(new CacheCandidate(ImmutableList.of(topNRanking)));
        assertThat(cacheController.getCachingCandidates(cacheProperties(false, true), subplans))
                .containsExactly(
                        new CacheCandidate(ImmutableList.of(secondProjection)),
                        new CacheCandidate(ImmutableList.of(firstProjection)));
    }

    private CanonicalSubplan createCanonicalAggregationSubplan(Set<CacheColumnId> groupByColumns)
    {
        CanonicalSubplan tableScanPlan = createCanonicalTableScanSubplan();

        return CanonicalSubplan.builderForChildSubplan(new AggregationKey(groupByColumns, ImmutableSet.of()), tableScanPlan)
                .originalPlanNode(new ValuesNode(PLAN_NODE_ID, 0))
                .originalSymbolMapping(ImmutableBiMap.of())
                .assignments(ImmutableMap.of())
                .pullableConjuncts(ImmutableSet.of())
                .groupByColumns(groupByColumns)
                .build();
    }

    private CanonicalSubplan createCanonicalTopNRankingSubplan(List<CacheColumnId> partitionBy, Map<CacheColumnId, SortOrder> orderBy, RankingType rankingType, int maxRankingPerPartition)
    {
        CanonicalSubplan tableScanPlan = createCanonicalTableScanSubplan();

        return CanonicalSubplan.builderForChildSubplan(new TopNRankingKey(partitionBy, orderBy.keySet().stream().toList(), orderBy, rankingType, maxRankingPerPartition, ImmutableSet.of()), tableScanPlan)
                .originalPlanNode(new ValuesNode(PLAN_NODE_ID, 0))
                .originalSymbolMapping(ImmutableBiMap.of())
                .assignments(ImmutableMap.of())
                .pullableConjuncts(ImmutableSet.of())
                .build();
    }

    private CanonicalSubplan createCanonicalTopNSubplan(Map<CacheColumnId, SortOrder> orderBy, long count)
    {
        CanonicalSubplan tableScanPlan = createCanonicalTableScanSubplan();

        return CanonicalSubplan.builderForChildSubplan(new CanonicalSubplan.TopNKey(orderBy.keySet().stream().toList(), orderBy, count, ImmutableSet.of()), tableScanPlan)
                .originalPlanNode(new ValuesNode(PLAN_NODE_ID, 0))
                .originalSymbolMapping(ImmutableBiMap.of())
                .assignments(ImmutableMap.of())
                .pullableConjuncts(ImmutableSet.of())
                .build();
    }

    private static CanonicalSubplan createCanonicalTableScanSubplan()
    {
        return createCanonicalTableScanSubplan(PLAN_NODE_ID, TupleDomain.all());
    }

    private static CanonicalSubplan createCanonicalTableScanSubplan(PlanNodeId planNodeId, TupleDomain<CacheColumnId> enforcedConstraint)
    {
        return CanonicalSubplan.builderForTableScan(
                        new ScanFilterProjectKey(TABLE_ID, ImmutableSet.of()),
                        ImmutableMap.of(),
                        enforcedConstraint,
                        TABLE_HANDLE,
                        TABLE_ID,
                        false,
                        planNodeId)
                .originalPlanNode(new ValuesNode(PLAN_NODE_ID, 0))
                .originalSymbolMapping(ImmutableBiMap.of())
                .assignments(ImmutableMap.of())
                .pullableConjuncts(ImmutableSet.of())
                .build();
    }

    private Session cacheProperties(boolean cacheAggregations, boolean cacheProjections)
    {
        return testSessionBuilder()
                .setSystemProperty(SUBQUERY_CACHE_AGGREGATIONS_ENABLED, Boolean.toString(cacheAggregations))
                .setSystemProperty(SUBQUERY_CACHE_PROJECTIONS_ENABLED, Boolean.toString(cacheProjections))
                .build();
    }
}
