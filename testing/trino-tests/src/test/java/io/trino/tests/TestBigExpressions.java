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
import java.util.UUID;
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
        String query =
                """
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
        }
        assertQuery("SELECT COALESCE(%s, %s) FROM nation".formatted(joiner, joiner));
    }

    // This test is expected to fail when compiler.columnar-filter-sub-expression-evaluation.enabled=false
    @Test
    public void testHighInputCountExpression()
    {
        int inputColumnCount = 150;
        StringJoiner values = new StringJoiner(", ");
        StringJoiner columnNames = new StringJoiner(", ");
        for (int i = 0; i < inputColumnCount; i++) {
            values.add("CAST(rand(1000) AS varchar)");
            columnNames.add("column_%s".formatted(i));
        }
        StringJoiner subExpressions = new StringJoiner(" || ");
        for (int i = 0; i < inputColumnCount / 2; i++) {
            subExpressions.add(
                    """
                    CASE
                        WHEN NOT m.column_%s IS NULL
                            THEN '%s:' || m.column_%s || ':' || CAST(m.column_%s AS varchar(50))|| ';'
                            else ''
                    END
                    """.formatted(i, UUID.randomUUID(), i + 1, i));
        }
        assertQuery(
                """
                WITH
                inputs AS (SELECT * FROM (VALUES (%s, 'natural values hash')) AS t(%s, natural_values_hash))
                ,hashes as (SELECT natural_values_hash, (%s) AS hash_string FROM inputs m)
                SELECT natural_values_hash FROM hashes WHERE natural_values_hash <> to_base64(md5(CAST(hash_string AS varbinary)))
                """.formatted(values, columnNames, subExpressions),
                "VALUES 'natural values hash'");
    }

    @Test
    public void testCaseWithLambdaAfterComplexityThreshold()
    {
        // Regression test for ENG-19683: a lambda expression (any_match) in a CASE branch that gets
        // compiled after the per-method complexity threshold caused a COMPILER_ERROR:
        // "Variable 'this' has not been assigned a slot".
        // The any_match branch is written first so it is compiled last (branches are processed in
        // reversed order), after the following simple branches push complexity past the threshold
        // and force the branch into an extracted method.
        StringBuilder caseBuilder = new StringBuilder("CASE");
        caseBuilder.append(" WHEN any_match(ARRAY[nationkey], y -> y > 20) THEN 999999");
        for (int i = 0; i < 250; i++) {
            caseBuilder.append(" WHEN nationkey = %d THEN %d".formatted(i, i * 10));
        }
        caseBuilder.append(" ELSE -1 END");
        String caseExpression = caseBuilder.toString();

        // test projection: any_match(nationkey > 20) selects nationkey 21-24, otherwise nationkey * 10
        assertQuery(
                "SELECT %s FROM nation".formatted(caseExpression),
                "SELECT CASE WHEN nationkey > 20 THEN 999999 ELSE nationkey * 10 END FROM nation");
        // test filter
        assertQuery(
                "SELECT nationkey FROM nation WHERE %s = 999999".formatted(caseExpression),
                "SELECT nationkey FROM nation WHERE nationkey > 20");
    }

    @Test
    public void testNestedLambdaExtractedIntoChunkClass()
    {
        // Regression test for the ENG-19683 variant where a lambda-bearing expression is extracted
        // into a method that lives in a chunk class (not the main class). At default compiler config
        // this requires a large query: enough extracted methods to fill the main class and spill into
        // chunk classes, plus a nested extraction (a big lambda-bearing branch) landing in a chunk.
        // There the extracted method's own 'this' is the chunk instance, so the lambda's receiver must
        // be resolved via the chunk's '__main' field. Before the fix this produced invalid bytecode
        // that failed at lambda link time with LambdaConversionException / an invalid-receiver error.
        //
        // Shape: an OUTER case whose first branch (evaluated for nationkey=5) returns a big INNER case
        // containing an any_match, preceded by many filler branches that fill methods and reach chunks.
        String actual = nestedLambdaChunkExpression("any_match(ARRAY[nationkey], y -> y > 0)");
        String expected = nestedLambdaChunkExpression("nationkey > 0");
        assertQuery(
                "SELECT %s FROM nation".formatted(actual),
                "SELECT %s FROM nation".formatted(expected));
    }

    private static String nestedLambdaChunkExpression(String predicate)
    {
        StringBuilder inner = new StringBuilder("CASE");
        inner.append(" WHEN %s THEN 999999".formatted(predicate));
        for (int i = 0; i < 250; i++) {
            inner.append(" WHEN nationkey = %d THEN %d".formatted(i, i * 10));
        }
        inner.append(" ELSE -1 END");

        StringBuilder outer = new StringBuilder("CASE");
        // first branch fires for nationkey=5, so INNER (and its lambda) is evaluated at runtime
        outer.append(" WHEN nationkey = 5 THEN (%s)".formatted(inner));
        for (int i = 0; i < 400; i++) {
            outer.append(" WHEN nationkey = %d THEN %d".formatted(i, i * 100));
        }
        outer.append(" ELSE -2 END");
        return outer.toString();
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
