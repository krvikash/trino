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
package io.trino.sql.query;

import io.trino.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.SystemSessionProperties.PUSH_AGGREGATION_INTO_VALUES_ENABLED;
import static io.trino.SystemSessionProperties.REWRITE_SUM_WITH_LITERAL_ENABLED;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Behavior-equivalence tests for {@code RewriteSumWithLiteralAsSumAndCount}
 * across every supported numeric column type, including overflow cases.
 *
 * <p>The rule currently applies only to TINYINT and SMALLINT columns. INTEGER
 * and BIGINT are excluded because the rewrite's {@code sum(col)} accumulator
 * could overflow BIGINT where the original's per-row-shifted accumulator would
 * have stayed in range. These tests lock in that contract: any future change
 * that extends the rule to wider types (e.g. by relying on column statistics)
 * must preserve the "rule-enabled produces the same result or same error as
 * rule-disabled" invariant exercised here.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRewriteSumWithLiteralOverflowBehavior
{
    private QueryAssertions assertions;
    private Session rewriteEnabled;
    private Session rewriteDisabled;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
        // push_aggregation_into_values_enabled crashes for VALUES with CAST /
        // arithmetic per-row expressions, independently of our rule; disable it
        // so both enabled/disabled sessions run the same optimisation chain.
        rewriteEnabled = Session.builder(assertions.getDefaultSession())
                .setSystemProperty(REWRITE_SUM_WITH_LITERAL_ENABLED, "true")
                .setSystemProperty(PUSH_AGGREGATION_INTO_VALUES_ENABLED, "false")
                .build();
        rewriteDisabled = Session.builder(assertions.getDefaultSession())
                .setSystemProperty(REWRITE_SUM_WITH_LITERAL_ENABLED, "false")
                .setSystemProperty(PUSH_AGGREGATION_INTO_VALUES_ENABLED, "false")
                .build();
    }

    @AfterAll
    public void cleanup()
    {
        assertions.close();
        assertions = null;
    }

    private void assertResultMatchesAcrossSessions(String query)
    {
        assertThat(assertions.query(rewriteEnabled, query))
                .result().matches(assertions.execute(rewriteDisabled, query));
    }

    private void assertOverflowUnderBothSessions(String query)
    {
        assertThat(assertions.query(rewriteEnabled, query))
                .failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        assertThat(assertions.query(rewriteDisabled, query))
                .failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
    }

    @Test
    public void testTinyintWithIntegerLiteralMatchesAcrossRule()
    {
        assertResultMatchesAcrossSessions(
                "SELECT sum(column + 1), sum(column - 1), sum(2 + column), sum(3 - column) "
                        + "FROM (VALUES CAST(127 AS TINYINT), CAST(-128 AS TINYINT), CAST(0 AS TINYINT)) t(column)");
    }

    @Test
    public void testTinyintPerRowOverflowWithNarrowLiteralThrowsUnderBothSessions()
    {
        // Inner add runs in TINYINT (user-forced narrow literal), so the rule must not
        // fire — otherwise the rewrite would mask the original's per-row overflow.
        assertOverflowUnderBothSessions(
                "SELECT sum(column + TINYINT '100') FROM (VALUES CAST(100 AS TINYINT)) t(column)");
    }

    @Test
    public void testSmallintWithIntegerLiteralMatchesAcrossRule()
    {
        assertResultMatchesAcrossSessions(
                "SELECT sum(column + 1), sum(column - 1), sum(10 + column), sum(5 - column) "
                        + "FROM (VALUES CAST(32767 AS SMALLINT), CAST(-32768 AS SMALLINT), CAST(0 AS SMALLINT), CAST(100 AS SMALLINT)) t(column)");
    }

    @Test
    public void testSmallintPerRowOverflowWithNarrowLiteralThrowsUnderBothSessions()
    {
        assertOverflowUnderBothSessions(
                "SELECT sum(column + SMALLINT '10000') FROM (VALUES CAST(30000 AS SMALLINT)) t(column)");
    }

    @Test
    public void testSmallintRewriteSucceedsWhereOriginalOverflowsPerRow()
    {
        // column + 2147483647 overflows INTEGER on the inner per-row add; the
        // original throws while the rewrite runs in BIGINT and returns the
        // correct answer. Two shifted sums on the same column so the rule's
        // multi-match gate fires. Divergence is accepted as benign.
        String query = "SELECT sum(column + 2147483647), sum(column + 1) FROM (VALUES CAST(1 AS SMALLINT)) t(column)";
        assertThat(assertions.query(rewriteDisabled, query))
                .failure().hasErrorCode(NUMERIC_VALUE_OUT_OF_RANGE);
        assertThat(assertions.query(rewriteEnabled, query))
                .matches("VALUES (BIGINT '2147483648', BIGINT '2')");
    }

    @Test
    public void testIntegerNormalMatchesAcrossRule()
    {
        assertResultMatchesAcrossSessions(
                "SELECT sum(column + 1), sum(column - 1), sum(5 + column), sum(7 - column) "
                        + "FROM (VALUES INTEGER '1000', INTEGER '-500', INTEGER '0') t(column)");
    }

    @Test
    public void testIntegerPerRowOverflowThrowsUnderBothSessions()
    {
        assertOverflowUnderBothSessions(
                "SELECT sum(column + 1) FROM (VALUES INTEGER '2147483647') t(column)");
    }

    @Test
    public void testBigintNormalMatchesAcrossRule()
    {
        assertResultMatchesAcrossSessions(
                "SELECT sum(column + 1), sum(column - 1), sum(5 + column), sum(7 - column) "
                        + "FROM (VALUES BIGINT '1000', BIGINT '-500', BIGINT '0') t(column)");
    }

    @Test
    public void testBigintPerRowOverflowThrowsUnderBothSessions()
    {
        assertOverflowUnderBothSessions(
                "SELECT sum(column + 1) FROM (VALUES BIGINT '9223372036854775807') t(column)");
    }

    @Test
    public void testBigintAccumulatorOverflowThrowsUnderBothSessions()
    {
        assertOverflowUnderBothSessions(
                "SELECT sum(column + 1) "
                        + "FROM (VALUES BIGINT '5000000000000000000', BIGINT '5000000000000000000') t(column)");
    }
}
