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
package io.trino.tests;

import io.trino.Session;
import io.trino.execution.DynamicFilterConfig;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.newir.Program;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import static com.google.common.base.Verify.verify;
import static io.trino.sql.query.QueryAssertions.ProgramAssert.newProgramAssert;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEnforceSingleRowWithCteReuse
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(new DynamicFilterConfig().isEnableDynamicFiltering(), "this class assumes dynamic filtering is enabled by default");
        return TpchQueryRunner.builder()
                .addExtraProperty("optimizer.reuse-common-subqueries", "true")
                .withExchange("filesystem")
                .build();
    }

    @Test
    public void testEnforceSingleRowWithCteReuseWithNoResidualFilter()
    {
        String query = """
                 SELECT name FROM region WHERE regionkey = (SELECT min(regionkey) FROM nation where nationkey = 8 GROUP BY nationkey)
                 UNION ALL
                 SELECT name FROM region WHERE regionkey = (SELECT max(regionkey) FROM nation where nationkey = 8 GROUP BY nationkey)
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedEnforceSingleRowOperationCount(2);

        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedEnforceSingleRowOperationCount(1);

        assertQuery(query, "VALUES ('ASIA'), ('ASIA')");
    }

    @Test
    public void testEnforceSingleRowWithCteReuseWithResidualFilterForSpecificBranch()
    {
        String query = """
                 SELECT name FROM region WHERE regionkey = (SELECT min(regionkey) FROM nation where nationkey = 8 GROUP BY nationkey)
                 UNION ALL
                 SELECT name FROM region WHERE regionkey = (SELECT min(regionkey) FROM nation where nationkey = 8 GROUP BY nationkey)
                 UNION ALL
                 SELECT name FROM region WHERE regionkey = (SELECT max(regionkey) FROM nation where nationkey = 2 GROUP BY nationkey)
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedEnforceSingleRowOperationCount(3);

        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedEnforceSingleRowOperationCount(2);

        assertQuery(query, "VALUES ('AMERICA'), ('ASIA'), ('ASIA')");
    }

    @Test
    public void testEnforceSingleRowWithCteReuseWithResidualFilterForEachBranch()
    {
        String query = """
                 SELECT name FROM region WHERE regionkey = (SELECT min(regionkey) FROM nation where nationkey = 8 GROUP BY nationkey)
                 UNION ALL
                 SELECT name FROM region WHERE regionkey = (SELECT max(regionkey) FROM nation where nationkey = 2 GROUP BY nationkey)
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedEnforceSingleRowOperationCount(2);

        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedEnforceSingleRowOperationCount(2);

        assertQuery(query, "VALUES ('AMERICA'), ('ASIA')");
    }

    @Test
    public void testEnforceSingleRowWithCteReuseWithEmptySource()
    {
        String query = """
                 SELECT name, (SELECT min(regionkey) FROM nation WHERE nationkey = -8 GROUP BY nationkey) FROM region WHERE regionkey = 2
                 UNION ALL
                 SELECT name, (SELECT min(regionkey) FROM nation WHERE nationkey = -8 GROUP BY nationkey) FROM region WHERE regionkey = 2
                """;

        assertThat(newProgramAssert(getProgramForDisabledCteReuse(query)))
                .expectedEnforceSingleRowOperationCount(2);

        assertThat(newProgramAssert(getProgramForEnabledCteReuse(query)))
                .expectedEnforceSingleRowOperationCount(1);

        assertQuery(query, "VALUES ('ASIA', NULL), ('ASIA', NULL)");
    }

    @Test
    public void testEnforceSingleRowWithCteReuseWithMultipleRows()
    {
        String query = """
                 SELECT name, (SELECT min(regionkey) FROM nation GROUP BY nationkey) FROM region WHERE regionkey = 2
                 UNION ALL
                 SELECT name, (SELECT min(regionkey) FROM nation GROUP BY nationkey) FROM region WHERE regionkey = 2
                """;

        assertQueryFails(query, "Scalar sub-query has returned multiple rows");
    }

    private Program getProgramForDisabledCteReuse(String query)
    {
        return getQueryRunner().executeWithPlan(getSessionForDisabledCteReuse(), query)
                .queryPlan()
                .map(plan -> ProgramBuilder.buildProgram(plan.getRoot())).orElseThrow();
    }

    private Program getProgramForEnabledCteReuse(String query)
    {
        return getQueryRunner().executeWithPlan(getSession(), query).program().orElseThrow();
    }

    private Session getSessionForDisabledCteReuse()
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty("reuse_common_subqueries", "false")
                .build();
    }
}
