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

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

public class TestExpressionCodeSplit
        extends TestLocalEngineOnlyQueries
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createQueryRunner(ImmutableMap.of(
                "compiler.row-expression-max-method-complexity", "0",
                "compiler.row-expression-max-methods-per-class", "0"));
    }

    @Test
    public void testComplexExpressionWithLambda()
    {
        // We want to test if lambda expressions still work after the call is executed from a chunk class.
        // For this we use AND, and CASE, which are both splittable, and nest the lambda invocations inside.

        // test filter
        assertQuery(
                """
                SELECT nationkey FROM nation
                WHERE name LIKE 'A%' AND
                    CASE ANY_MATCH(ARRAY[nationkey], n ->  n > 1000)
                        WHEN
                            TRUE THEN 1
                        ELSE (
                            CASE ANY_MATCH(ARRAY[nationkey], n ->  n < 3)
                                WHEN
                                    TRUE THEN -1
                                ELSE 0
                            END)
                    END < 0""",
                "VALUES (0), (1)");

        // test projection
        assertQuery(
                """
                SELECT nationkey, name LIKE 'A%' AND
                            CASE ANY_MATCH(ARRAY[nationkey], n ->  n > 1000)
                                WHEN
                                    TRUE THEN 1
                                ELSE (
                                    CASE ANY_MATCH(ARRAY[nationkey], n ->  n < 3)
                                        WHEN
                                            TRUE THEN -1
                                        ELSE 0
                                    END)
                            END < 0
                        FROM nation WHERE nationkey < 3""",
                "VALUES (0, true), (1, true), (2, false)");
    }

    @Test
    public void testComplexExpressionWithCallSiteBindings()
    {
        // We want to test if call site bindings still work after the call is executed from a chunk class.
        // For this we use AND, and CASE, which are both splittable, and nest the UDF function calls, which add call site bindings, inside.

        // test filter
        assertQuery(
                """
                WITH
                  FUNCTION len(x varchar)
                    RETURNS BIGINT
                    RETURN length(x)
                SELECT nationkey FROM nation
                WHERE name LIKE '%' AND
                    CASE length(name)
                        WHEN 4 THEN 1
                        ELSE (
                            CASE
                                WHEN len(comment) < 40 THEN -1
                                ELSE 0
                            END)
                    END  < 0""",
                "VALUES (5), (6), (12)");

        // test projection
        assertQuery(
                """
                WITH
                  FUNCTION len(x varchar)
                    RETURNS BIGINT
                    RETURN length(x)
                SELECT nationkey, name LIKE '%' AND
                    CASE length(name)
                        WHEN 4 THEN 1
                        ELSE (
                            CASE
                                WHEN len(comment) < 40 THEN -1
                                ELSE 0
                            END)
                    END  < 0
                FROM nation WHERE nationkey < 7""",
                "VALUES (0, false), (1, false), (2, false), (3, false), (4, false), (5, true), (6, true)");
    }

    @Test
    public void testBetweenExpression()
    {
        // test filter
        assertQuery(
                """
                SELECT nationkey FROM nation
                WHERE length(name) BETWEEN 5 AND 6 AND nationkey <= 5""",
                "VALUES (2), (3), (4)");

        // test projection
        assertQuery(
                """
                SELECT nationkey, length(name) BETWEEN 5 AND 6
                FROM nation WHERE nationkey <= 5""",
                "VALUES (0, false), (1, false), (2, true), (3, true), (4, true), (5, false)");
    }
}
