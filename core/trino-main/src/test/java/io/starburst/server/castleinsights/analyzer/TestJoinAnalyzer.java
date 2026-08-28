/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.server.castleinsights.analyzer;

import io.starburst.server.castleinsights.CastleQueryAnalysis;
import io.starburst.server.castleinsights.model.CastleInsight;
import io.starburst.server.castleinsights.model.JoinInsight;
import io.trino.spi.QueryId;
import io.trino.testing.StandaloneQueryRunner;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.starburst.server.castleinsights.CastleInsightsTestHelper.createTpchQueryRunner;
import static io.starburst.server.castleinsights.CastleInsightsTestHelper.finalQueryInfo;
import static io.starburst.server.castleinsights.model.Cardinality.MANY_TO_MANY;
import static io.starburst.server.castleinsights.model.Cardinality.MANY_TO_ONE;
import static io.starburst.server.castleinsights.model.JoinType.INNER;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.assertj.core.api.Assertions.assertThat;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJoinAnalyzer
{
    private final JoinAnalyzer analyzer = new JoinAnalyzer();
    private StandaloneQueryRunner queryRunner;

    @BeforeAll
    public void setUp()
    {
        queryRunner = createTpchQueryRunner();
    }

    @AfterAll
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testManyToOneJoinOnForeignKey()
    {
        // Every order references exactly one customer: orders (probe/left, many) to customer (build/right, one).
        JoinInsight insight = getJoinInsight("SELECT o.orderkey FROM orders o JOIN customer c ON o.custkey = c.custkey");

        assertThat(insight.left().table()).isEqualTo("orders");
        assertThat(insight.leftColumns()).containsExactly("custkey");
        assertThat(insight.right().table()).isEqualTo("customer");
        assertThat(insight.rightColumns()).containsExactly("custkey");
        assertThat(insight.joinType()).isEqualTo(INNER);
        assertThat(insight.cardinality()).contains(MANY_TO_ONE);
    }

    @Test
    public void testManyToManyJoinOnNonUniqueKeys()
    {
        // nationkey is non-unique on both sides: many customers per nation, many suppliers per nation.
        JoinInsight insight = getJoinInsight("SELECT c.name FROM customer c JOIN supplier s ON c.nationkey = s.nationkey");

        assertThat(insight.cardinality()).contains(MANY_TO_MANY);
    }

    @Test
    public void testOuterJoinDoesNotInferManyToManyFromPreservedRows()
    {
        // A left outer join on a foreign key: custkey is unique on the build (customer) side, so this is
        // MANY_TO_ONE. The unmatched preserved rows must not push the output-vs-build test into MANY_TO_MANY.
        JoinInsight insight = getJoinInsight("SELECT o.orderkey FROM orders o LEFT JOIN customer c ON o.custkey = c.custkey");

        assertThat(insight.cardinality()).contains(MANY_TO_ONE);
    }

    @Test
    public void testColumnNamesSurviveAProjection()
    {
        // Casting the join key forces a projection between the scan and the join; the column name must
        // still trace through it.
        JoinInsight insight = getJoinInsight("SELECT o.orderkey FROM orders o JOIN customer c ON CAST(o.custkey AS bigint) = c.custkey");

        assertThat(insight.left().table()).isEqualTo("orders");
        assertThat(insight.leftColumns()).containsExactly("custkey");
        assertThat(insight.right().table()).isEqualTo("customer");
        assertThat(insight.rightColumns()).containsExactly("custkey");
    }

    @Test
    public void testColumnNamesSurviveNestedCasts()
    {
        // Two stacked casts on the join key; the column name must still trace through both.
        JoinInsight insight = getJoinInsight("SELECT o.orderkey FROM orders o JOIN customer c ON CAST(CAST(o.custkey AS bigint) AS bigint) = c.custkey");

        assertThat(insight.leftColumns()).containsExactly("custkey");
        assertThat(insight.rightColumns()).containsExactly("custkey");
    }

    @Test
    public void testExpressionJoinIsSkipped()
    {
        // The join key is a computed expression, not a column, so no insight is emitted.
        assertThat(analyze("SELECT o.orderkey FROM orders o JOIN customer c ON o.custkey + 1 = c.custkey"))
                .isEmpty();
    }

    private JoinInsight getJoinInsight(@Language("SQL") String sql)
    {
        return (JoinInsight) analyze(sql).stream().collect(onlyElement());
    }

    private List<CastleInsight> analyze(@Language("SQL") String sql)
    {
        QueryId queryId = queryRunner.executeWithQueryId(TEST_SESSION, sql).queryId();
        return analyzer.analyze(new CastleQueryAnalysis(finalQueryInfo(queryRunner, queryId)));
    }
}
