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

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunner;
import org.junit.jupiter.api.Test;

import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;

public class TestBigExpressions
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunner.builder().setWorkerCount(1).build();
    }

    @Test
    public void testComplexSwitch()
    {
        // query close to the 1MB query size limit
        // test projection
        assertQuery("SELECT %s FROM nation".formatted(generateCase("comment", 5, 5)));
        // test filter
        assertQuery("SELECT * FROM nation WHERE %s = random(7)".formatted(generateCase("comment", 5, 5)));
    }

    @Test
    public void testComplexSwitchFilterTranslatedToOr()
    {
        StringBuilder mappingCaseBuilder = new StringBuilder("CASE ");
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < 60; i++) {
            mappingCaseBuilder.append(" WHEN COL1 = 'V' AND COL2 = %s THEN %s".formatted(
                    random.nextInt(300),
                    random.nextInt(8) + 1));
        }
        String mappingCase = mappingCaseBuilder.append(" ELSE -1 END").toString();
        String query = """
                       select *
                       from (select 'V' as COL1, nationkey * 20 as COL2 from nation limit 10) t
                       WHERE CASE
                           WHEN %s = 1 THEN 'MATCH'
                           WHEN %s = 2 THEN 'MATCH'
                           WHEN %s = 3 THEN 'MATCH'
                           WHEN %s = 4 THEN 'MATCH'
                           WHEN %s = 5 THEN 'MATCH'
                           WHEN %s = 6 THEN 'MATCH'
                           WHEN %s = 7 THEN 'MATCH'
                           WHEN %s = 8 THEN 'MATCH'
                           ELSE 'NO MATCH' END = 'MATCH'
                       """.formatted(mappingCase, mappingCase, mappingCase, mappingCase, mappingCase, mappingCase, mappingCase, mappingCase);
        assertQuery(query);
    }

    @Test
    public void testComplexCoalesceExpression()
    {
        StringJoiner joiner = new StringJoiner(", ");
        for (int i = 0; i < 1000; i++) {
            joiner.add("COALESCE(POWER(nationkey * 2, %d) + POWER(nationkey * 2, 1), POWER(nationkey * 2, 0), POWER(nationkey * 2, 1))".formatted(i));
        };
        assertQuery("SELECT COALESCE(%s, %s) FROM nation".formatted(joiner, joiner));
    }

    private static String generateCase(String column, int whenCases, int depth)
    {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb = new StringBuilder("CASE ");
        for (int i = 0; i < whenCases; i++) {
            sb.append(" WHEN %s IN ('%s') THEN (%s)".formatted(
                    column,
                    random.nextInt(1000),
                    depth == 0 ? random.nextInt() : generateCase(column, whenCases, depth - 1)));
        }
        return sb.append(" ELSE -1 END").toString();
    }
}
