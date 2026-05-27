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
package io.trino.operator.gpu;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.trino.FullConnectorSession;
import io.trino.Session;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.OutputFactory;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.ErrorCode;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.planner.InternalDynamicFilter;
import io.trino.sql.planner.Symbol;
import io.trino.testing.PageConsumerOperator.PageConsumerOutputFactory;
import io.trino.testing.PlanTester;
import jakarta.annotation.Nullable;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGpuCasts
{
    private static final DecimalType DECIMAL_13_2 = createDecimalType(13, 2);
    private static final DecimalType DECIMAL_27_5 = createDecimalType(27, 5);

    private PlanTester planTester;
    private TestingFunctionResolution functionResolution;
    private FullConnectorSession fullConnectorSession;

    @BeforeAll
    void setUp()
    {
        Session session = testSessionBuilder().build();
        planTester = PlanTester.create(session);
        functionResolution = new TestingFunctionResolution(planTester.getTransactionManager(), planTester.getPlannerContext());
        fullConnectorSession = new FullConnectorSession(session, session.getIdentity().toConnectorIdentity());
    }

    @BeforeAll
    static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @AfterAll
    void tearDown()
    {
        if (planTester != null) {
            planTester.close();
            planTester = null;
        }
    }

    @Test
    void testCastFromTinyint()
    {
        String[] values = {
                "TINYINT '-1'",
                "TINYINT '0'",
                "TINYINT '1'",
                "TINYINT '-128'",
                "TINYINT '127'",
                "CAST(NULL AS TINYINT)",
        };
        assertCastSucceedsForAll(TINYINT, TINYINT, values);
        assertCastSucceedsForAll(TINYINT, SMALLINT, values);
        assertCastSucceedsForAll(TINYINT, INTEGER, values);
        assertCastSucceedsForAll(TINYINT, BIGINT, values);
        assertCastSucceedsForAll(TINYINT, REAL, values);
        assertCastSucceedsForAll(TINYINT, DOUBLE, values);
        assertCastSucceedsForAll(TINYINT, DECIMAL_13_2, values);
        assertCastSucceedsForAll(TINYINT, DECIMAL_27_5, values);
        // NUMBER is not yet supported on GPU
        assertThat(gpuCast(TINYINT, NUMBER)).isNotSupported();
        // VARCHAR(N) supported when N is large enough to hold sign + 3 digits
        assertCastSucceedsForAll(TINYINT, createVarcharType(4), values);
        assertCastSucceedsForAll(TINYINT, VARCHAR, values);
        assertThat(gpuCast(TINYINT, createVarcharType(3))).isNotSupported();
    }

    @Test
    void testCastFromSmallint()
    {
        String[] inRange = {
                "SMALLINT '-1'",
                "SMALLINT '0'",
                "SMALLINT '1'",
                "SMALLINT '-32768'",
                "SMALLINT '32767'",
                "CAST(NULL AS SMALLINT)",
        };
        String[] tinyintInRange = {
                "SMALLINT '-128'",
                "SMALLINT '127'",
                "CAST(NULL AS SMALLINT)",
        };
        String[] tinyintOutOfRange = {
                "SMALLINT '-129'",
                "SMALLINT '128'",
                "SMALLINT '-32768'",
                "SMALLINT '32767'",
        };
        assertCastSucceedsForAll(SMALLINT, TINYINT, tinyintInRange);
        for (String oor : tinyintOutOfRange) {
            assertThat(gpuCast(SMALLINT, TINYINT)).failsCorrectly(oor);
        }
        assertCastSucceedsForAll(SMALLINT, SMALLINT, inRange);
        assertCastSucceedsForAll(SMALLINT, INTEGER, inRange);
        assertCastSucceedsForAll(SMALLINT, BIGINT, inRange);
        assertCastSucceedsForAll(SMALLINT, REAL, inRange);
        assertCastSucceedsForAll(SMALLINT, DOUBLE, inRange);
        assertCastSucceedsForAll(SMALLINT, DECIMAL_13_2, inRange);
        assertCastSucceedsForAll(SMALLINT, DECIMAL_27_5, inRange);
        assertThat(gpuCast(SMALLINT, NUMBER)).isNotSupported();
        // VARCHAR(N) supported when N is large enough to hold sign + 5 digits
        assertCastSucceedsForAll(SMALLINT, createVarcharType(6), inRange);
        assertCastSucceedsForAll(SMALLINT, VARCHAR, inRange);
        assertThat(gpuCast(SMALLINT, createVarcharType(5))).isNotSupported();
    }

    @Test
    void testCastFromInteger()
    {
        String[] inRange = {
                "INTEGER '-1'",
                "INTEGER '0'",
                "INTEGER '1'",
                "INTEGER '-2147483648'",
                "INTEGER '2147483647'",
                "CAST(NULL AS INTEGER)",
        };
        String[] tinyintInRange = {"INTEGER '-128'", "INTEGER '127'", "CAST(NULL AS INTEGER)"};
        String[] tinyintOutOfRange = {"INTEGER '-129'", "INTEGER '128'", "INTEGER '-2147483648'", "INTEGER '2147483647'"};
        String[] smallintInRange = {"INTEGER '-32768'", "INTEGER '32767'", "CAST(NULL AS INTEGER)"};
        String[] smallintOutOfRange = {"INTEGER '-32769'", "INTEGER '32768'", "INTEGER '-2147483648'", "INTEGER '2147483647'"};
        assertCastSucceedsForAll(INTEGER, TINYINT, tinyintInRange);
        for (String oor : tinyintOutOfRange) {
            assertThat(gpuCast(INTEGER, TINYINT)).failsCorrectly(oor);
        }
        assertCastSucceedsForAll(INTEGER, SMALLINT, smallintInRange);
        for (String oor : smallintOutOfRange) {
            assertThat(gpuCast(INTEGER, SMALLINT)).failsCorrectly(oor);
        }
        assertCastSucceedsForAll(INTEGER, INTEGER, inRange);
        assertCastSucceedsForAll(INTEGER, BIGINT, inRange);
        assertCastSucceedsForAll(INTEGER, REAL, inRange);
        assertCastSucceedsForAll(INTEGER, DOUBLE, inRange);
        assertCastSucceedsForAll(INTEGER, DECIMAL_13_2, inRange);
        assertCastSucceedsForAll(INTEGER, DECIMAL_27_5, inRange);
        assertThat(gpuCast(INTEGER, NUMBER)).isNotSupported();
        // VARCHAR(N) supported when N is large enough to hold sign + 10 digits
        assertCastSucceedsForAll(INTEGER, createVarcharType(11), inRange);
        assertCastSucceedsForAll(INTEGER, VARCHAR, inRange);
        assertThat(gpuCast(INTEGER, createVarcharType(10))).isNotSupported();
    }

    @Test
    void testCastFromBigint()
    {
        String[] inRange = {
                "BIGINT '-1'",
                "BIGINT '0'",
                "BIGINT '1'",
                "BIGINT '-9223372036854775808'",
                "BIGINT '9223372036854775807'",
                "CAST(NULL AS BIGINT)",
        };
        String[] tinyintInRange = {"BIGINT '-128'", "BIGINT '127'", "CAST(NULL AS BIGINT)"};
        String[] tinyintOutOfRange = {"BIGINT '-129'", "BIGINT '128'", "BIGINT '-9223372036854775808'", "BIGINT '9223372036854775807'"};
        String[] smallintInRange = {"BIGINT '-32768'", "BIGINT '32767'", "CAST(NULL AS BIGINT)"};
        String[] smallintOutOfRange = {"BIGINT '-32769'", "BIGINT '32768'", "BIGINT '-9223372036854775808'", "BIGINT '9223372036854775807'"};
        String[] integerInRange = {"BIGINT '-2147483648'", "BIGINT '2147483647'", "CAST(NULL AS BIGINT)"};
        String[] integerOutOfRange = {"BIGINT '-2147483649'", "BIGINT '2147483648'", "BIGINT '-9223372036854775808'", "BIGINT '9223372036854775807'"};
        assertCastSucceedsForAll(BIGINT, TINYINT, tinyintInRange);
        for (String oor : tinyintOutOfRange) {
            assertThat(gpuCast(BIGINT, TINYINT)).failsCorrectly(oor);
        }
        assertCastSucceedsForAll(BIGINT, SMALLINT, smallintInRange);
        for (String oor : smallintOutOfRange) {
            assertThat(gpuCast(BIGINT, SMALLINT)).failsCorrectly(oor);
        }
        assertCastSucceedsForAll(BIGINT, INTEGER, integerInRange);
        for (String oor : integerOutOfRange) {
            assertThat(gpuCast(BIGINT, INTEGER)).failsCorrectly(oor);
        }
        // BIGINT (19 digits) does not fit DECIMAL(13,2) integer range (11 digits)
        assertThat(gpuCast(BIGINT, DECIMAL_13_2)).isNotSupported();
        assertCastSucceedsForAll(BIGINT, BIGINT, inRange);
        assertCastSucceedsForAll(BIGINT, REAL, inRange);
        assertCastSucceedsForAll(BIGINT, DOUBLE, inRange);
        assertCastSucceedsForAll(BIGINT, DECIMAL_27_5, inRange);
        assertThat(gpuCast(BIGINT, NUMBER)).isNotSupported();
        // VARCHAR(N) supported when N is large enough to hold sign + 19 digits
        assertCastSucceedsForAll(BIGINT, createVarcharType(20), inRange);
        assertCastSucceedsForAll(BIGINT, VARCHAR, inRange);
        assertThat(gpuCast(BIGINT, createVarcharType(19))).isNotSupported();
    }

    @Test
    void testCastFromDecimal13()
    {
        // DECIMAL(13, 2): range is -99999999999.99 .. 99999999999.99
        String[] values = {
                "CAST(-1 AS DECIMAL(13, 2))",
                "CAST(0 AS DECIMAL(13, 2))",
                "CAST(1 AS DECIMAL(13, 2))",
                "CAST(-99999999999.99 AS DECIMAL(13, 2))",
                "CAST(99999999999.99 AS DECIMAL(13, 2))",
                "CAST(NULL AS DECIMAL(13, 2))",
        };
        // Non-zero scale precludes lossless cast to any integer type
        assertThat(gpuCast(DECIMAL_13_2, TINYINT)).isNotSupported();
        assertThat(gpuCast(DECIMAL_13_2, SMALLINT)).isNotSupported();
        assertThat(gpuCast(DECIMAL_13_2, INTEGER)).isNotSupported();
        assertThat(gpuCast(DECIMAL_13_2, BIGINT)).isNotSupported();
        assertCastSucceedsForAll(DECIMAL_13_2, REAL, values);
        assertCastSucceedsForAll(DECIMAL_13_2, DOUBLE, values);
        assertCastSucceedsForAll(DECIMAL_13_2, DECIMAL_13_2, values);
        assertCastSucceedsForAll(DECIMAL_13_2, DECIMAL_27_5, values);
        assertThat(gpuCast(DECIMAL_13_2, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromDecimal27()
    {
        // DECIMAL(27, 5): range is -9999999999999999999999.99999 .. 9999999999999999999999.99999
        String[] values = {
                "CAST(-1 AS DECIMAL(27, 5))",
                "CAST(0 AS DECIMAL(27, 5))",
                "CAST(1 AS DECIMAL(27, 5))",
                "CAST(-9999999999999999999999.99999 AS DECIMAL(27, 5))",
                "CAST(9999999999999999999999.99999 AS DECIMAL(27, 5))",
                "CAST(NULL AS DECIMAL(27, 5))",
        };
        // Non-zero scale precludes lossless cast to any integer type
        assertThat(gpuCast(DECIMAL_27_5, TINYINT)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, SMALLINT)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, INTEGER)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, BIGINT)).isNotSupported();
        assertCastSucceedsForAll(DECIMAL_27_5, REAL, values);
        assertCastSucceedsForAll(DECIMAL_27_5, DOUBLE, values);
        // decimal(27,5) integer digits (22) exceed decimal(13,2) integer digits (11)
        assertThat(gpuCast(DECIMAL_27_5, DECIMAL_13_2)).isNotSupported();
        assertCastSucceedsForAll(DECIMAL_27_5, DECIMAL_27_5, values);
        assertThat(gpuCast(DECIMAL_27_5, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromReal()
    {
        // Round half-away-from-zero: -0.5 → -1, 0.5 → 1. Boundary values exercise the rounding boundary.
        String[] tinyintInRange = {"REAL '-128.0'", "REAL '127.0'", "REAL '-128.49'", "REAL '127.49'", "REAL '0.4'", "REAL '-0.4'", "CAST(NULL AS REAL)"};
        String[] tinyintFinitelyOutOfRange = {"REAL '128.0'", "REAL '-129.0'", "REAL '127.5'", "REAL '-128.5'", "REAL '1000.0'"};
        String[] smallintInRange = {"REAL '-32768.0'", "REAL '32767.0'", "REAL '0.0'", "CAST(NULL AS REAL)"};
        String[] smallintFinitelyOutOfRange = {"REAL '32768.0'", "REAL '-32769.0'", "REAL '32767.5'", "REAL '-32768.5'", "REAL '1.0E7'"};
        String[] integerInRange = {"REAL '-1.0'", "REAL '1.0'", "REAL '0.0'", "CAST(NULL AS REAL)"};
        // Float around 2^31 has ulp ≥ 256, so REAL '2.147483648E9' (= 2^31, smallest float past INT_MAX) is the OOR boundary.
        String[] integerFinitelyOutOfRange = {"REAL '2.147483648E9'", "REAL '-2.147483904E9'", "REAL '3.4028235E38'", "REAL '-3.4028235E38'"};
        String[] bigintInRange = {"REAL '-1.0'", "REAL '1.0'", "CAST(NULL AS REAL)"};
        String[] bigintFinitelyOutOfRange = {"REAL '1e20'"};
        String[] nonFiniteAnyTarget = {"REAL 'NaN'", "REAL 'Infinity'", "REAL '-Infinity'"};

        // tinyint
        assertCastSucceedsForAll(REAL, TINYINT, tinyintInRange);
        for (String oor : tinyintFinitelyOutOfRange) {
            assertThat(gpuCast(REAL, TINYINT)).failsCorrectly(oor);
        }
        for (String value : nonFiniteAnyTarget) {
            assertThat(gpuCast(REAL, TINYINT)).failsCorrectly(value);
        }

        // smallint
        assertCastSucceedsForAll(REAL, SMALLINT, smallintInRange);
        for (String oor : smallintFinitelyOutOfRange) {
            assertThat(gpuCast(REAL, SMALLINT)).failsCorrectly(oor);
        }
        for (String value : nonFiniteAnyTarget) {
            assertThat(gpuCast(REAL, SMALLINT)).failsCorrectly(value);
        }

        // integer
        assertCastSucceedsForAll(REAL, INTEGER, integerInRange);
        for (String oor : integerFinitelyOutOfRange) {
            assertThat(gpuCast(REAL, INTEGER)).failsCorrectly(oor);
        }
        for (String value : nonFiniteAnyTarget) {
            assertThat(gpuCast(REAL, INTEGER)).failsCorrectly(value);
        }

        // bigint
        assertCastSucceedsForAll(REAL, BIGINT, bigintInRange);
        for (String value : bigintFinitelyOutOfRange) {
            assertThat(gpuCast(REAL, BIGINT)).failsCorrectly(value);
        }
        for (String value : nonFiniteAnyTarget) {
            assertThat(gpuCast(REAL, BIGINT)).failsCorrectly(value);
        }

        String[] values = {
                "REAL '-1.0'",
                "REAL '0.0'",
                "REAL '1.0'",
                "REAL '-3.4028235E38'",
                "REAL '3.4028235E38'",
                "REAL 'NaN'",
                "REAL 'Infinity'",
                "REAL '-Infinity'",
                "CAST(NULL AS REAL)",
        };

        // real
        assertCastSucceedsForAll(REAL, REAL, values);

        // double
        assertCastSucceedsForAll(REAL, DOUBLE, values);

        // decimal
        assertThat(gpuCast(REAL, DECIMAL_13_2)).isNotSupported();
        assertThat(gpuCast(REAL, DECIMAL_27_5)).isNotSupported();

        // number
        assertThat(gpuCast(REAL, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromDouble()
    {
        String[] tinyintInRange = {"DOUBLE '-128.0'", "DOUBLE '127.0'", "DOUBLE '-128.49'", "DOUBLE '127.49'", "DOUBLE '0.4'", "DOUBLE '-0.4'", "CAST(NULL AS DOUBLE)"};
        String[] tinyintFinitelyOutOfRange = {"DOUBLE '128.0'", "DOUBLE '-129.0'", "DOUBLE '127.5'", "DOUBLE '-128.5'", "DOUBLE '1000.0'"};
        String[] smallintInRange = {"DOUBLE '-32768.0'", "DOUBLE '32767.0'", "DOUBLE '0.0'", "CAST(NULL AS DOUBLE)"};
        String[] smallintFinitelyOutOfRange = {"DOUBLE '32768.0'", "DOUBLE '-32769.0'", "DOUBLE '32767.5'", "DOUBLE '-32768.5'", "DOUBLE '1.0E7'"};
        String[] integerInRange = {"DOUBLE '-1.0'", "DOUBLE '1.0'", "DOUBLE '-2147483648.0'", "DOUBLE '2147483647.0'", "CAST(NULL AS DOUBLE)"};
        String[] integerFinitelyOutOfRange = {"DOUBLE '2147483648.0'", "DOUBLE '-2147483649.0'", "DOUBLE '1.0E20'"};
        String[] bigintInRange = {"DOUBLE '-1.0'", "DOUBLE '1.0'", "CAST(NULL AS DOUBLE)"};
        String[] bigintFinitelyOutOfRange = {"DOUBLE '1.0E20'", "DOUBLE '-1.0E20'", "DOUBLE '1.7976931348623157E308'"};
        String[] nonFiniteAnyTarget = {"DOUBLE 'NaN'", "DOUBLE 'Infinity'", "DOUBLE '-Infinity'"};

        // tinyint
        assertCastSucceedsForAll(DOUBLE, TINYINT, tinyintInRange);
        for (String oor : tinyintFinitelyOutOfRange) {
            assertThat(gpuCast(DOUBLE, TINYINT)).failsCorrectly(oor);
        }
        for (String value : nonFiniteAnyTarget) {
            assertThat(gpuCast(DOUBLE, TINYINT)).failsCorrectly(value);
        }

        // smallint
        assertCastSucceedsForAll(DOUBLE, SMALLINT, smallintInRange);
        for (String oor : smallintFinitelyOutOfRange) {
            assertThat(gpuCast(DOUBLE, SMALLINT)).failsCorrectly(oor);
        }
        for (String value : nonFiniteAnyTarget) {
            assertThat(gpuCast(DOUBLE, SMALLINT)).failsCorrectly(value);
        }

        // integer
        assertCastSucceedsForAll(DOUBLE, INTEGER, integerInRange);
        for (String oor : integerFinitelyOutOfRange) {
            assertThat(gpuCast(DOUBLE, INTEGER)).failsCorrectly(oor);
        }
        for (String value : nonFiniteAnyTarget) {
            assertThat(gpuCast(DOUBLE, INTEGER)).failsCorrectly(value);
        }

        // bigint
        assertCastSucceedsForAll(DOUBLE, BIGINT, bigintInRange);
        for (String oor : bigintFinitelyOutOfRange) {
            assertThat(gpuCast(DOUBLE, BIGINT)).failsCorrectly(oor);
        }
        for (String value : nonFiniteAnyTarget) {
            assertThat(gpuCast(DOUBLE, BIGINT)).failsCorrectly(value);
        }

        String[] values = {
                "DOUBLE '-1.0'",
                "DOUBLE '0.0'",
                "DOUBLE '1.0'",
                "DOUBLE '-1.7976931348623157E308'",
                "DOUBLE '1.7976931348623157E308'",
                "DOUBLE 'NaN'",
                "DOUBLE 'Infinity'",
                "DOUBLE '-Infinity'",
                "CAST(NULL AS DOUBLE)",
        };

        // real
        assertCastSucceedsForAll(DOUBLE, REAL, values);

        // double
        assertCastSucceedsForAll(DOUBLE, DOUBLE, values);

        // decimal
        assertThat(gpuCast(DOUBLE, DECIMAL_13_2)).isNotSupported();
        assertThat(gpuCast(DOUBLE, DECIMAL_27_5)).isNotSupported();

        // number
        assertThat(gpuCast(DOUBLE, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromNumber()
    {
        // NUMBER is not yet supported as a GPU input type; all casts from it are unsupported.
        assertThat(gpuCast(NUMBER, TINYINT)).isNotSupported();
        assertThat(gpuCast(NUMBER, SMALLINT)).isNotSupported();
        assertThat(gpuCast(NUMBER, INTEGER)).isNotSupported();
        assertThat(gpuCast(NUMBER, BIGINT)).isNotSupported();
        assertThat(gpuCast(NUMBER, REAL)).isNotSupported();
        assertThat(gpuCast(NUMBER, DOUBLE)).isNotSupported();
        assertThat(gpuCast(NUMBER, DECIMAL_13_2)).isNotSupported();
        assertThat(gpuCast(NUMBER, DECIMAL_27_5)).isNotSupported();
        assertThat(gpuCast(NUMBER, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromVarchar()
    {
        String[] varchar5Values = {
                "CAST('hello' AS VARCHAR(5))",
                "CAST('' AS VARCHAR(5))",
                "CAST('a' AS VARCHAR(5))",
                "CAST(NULL AS VARCHAR(5))",
        };
        // Same width is a no-op widening
        assertCastSucceedsForAll(createVarcharType(5), createVarcharType(5), varchar5Values);
        // Widening to a larger bound
        assertCastSucceedsForAll(createVarcharType(5), createVarcharType(10), varchar5Values);
        // Widening to unbounded
        assertCastSucceedsForAll(createVarcharType(5), VARCHAR, varchar5Values);

        // Narrowing is not supported on GPU (would require length check / truncation)
        assertThat(gpuCast(createVarcharType(5), createVarcharType(4))).isNotSupported();
        // Unbounded source cannot be narrowed to a bounded target
        assertThat(gpuCast(VARCHAR, createVarcharType(5))).isNotSupported();

        String[] unboundedValues = {
                "CAST('hello' AS VARCHAR)",
                "CAST('' AS VARCHAR)",
                "CAST(NULL AS VARCHAR)",
        };
        assertCastSucceedsForAll(VARCHAR, VARCHAR, unboundedValues);

        // VARCHAR -> non-string targets: not supported on GPU
        assertThat(gpuCast(createVarcharType(20), TINYINT)).isNotSupported();
        assertThat(gpuCast(createVarcharType(20), SMALLINT)).isNotSupported();
        assertThat(gpuCast(createVarcharType(20), INTEGER)).isNotSupported();
        assertThat(gpuCast(createVarcharType(20), BIGINT)).isNotSupported();
        assertThat(gpuCast(createVarcharType(20), REAL)).isNotSupported();
        assertThat(gpuCast(createVarcharType(20), DOUBLE)).isNotSupported();
        assertThat(gpuCast(createVarcharType(20), DECIMAL_13_2)).isNotSupported();
        assertThat(gpuCast(createVarcharType(20), DECIMAL_27_5)).isNotSupported();
        assertThat(gpuCast(createVarcharType(20), NUMBER)).isNotSupported();
    }

    private void assertCastSucceedsForAll(Type from, Type to, @Language("SQL") String[] sqlValues)
    {
        GpuCastAssert assertion = assertThat(gpuCast(from, to));
        for (String sql : sqlValues) {
            assertion.executesCorrectly(sql);
        }
    }

    private AssertProvider<GpuCastAssert> gpuCast(Type from, Type to)
    {
        return () -> new GpuCastAssert(from, to);
    }

    /**
     * Fluent assertion for a single (from, to) cast pair.
     * The GPU compilation happens lazily on the first method call and is cached.
     * Per-value assertions reuse the same compiled expression.
     */
    private class GpuCastAssert
            extends AbstractAssert<GpuCastAssert, String>
    {
        private final Type from;
        private final Type to;
        private final Expression castExpression;
        private final Map<Symbol, Integer> layout;
        private final Supplier<Optional<CompiledExpression>> compiled;

        GpuCastAssert(Type from, Type to)
        {
            super(from + " -> " + to, GpuCastAssert.class);
            this.from = from;
            this.to = to;
            this.castExpression = new Cast(new Reference(from, "ref0"), to);
            this.layout = ImmutableMap.of(new Symbol(from, "ref0"), 0);
            this.compiled = Suppliers.memoize(() -> compileExpression(castExpression, layout));
        }

        /**
         * Terminal: asserts the cast cannot be compiled to a GPU expression.
         */
        public void isNotSupported()
        {
            if (compiled.get().isPresent()) {
                throw new AssertionError(format("Expected cast %s -> %s to be unsupported on GPU, but it compiled", from, to));
            }
        }

        /**
         * Asserts the cast of the given SQL-evaluated value produces the same result on GPU and CPU.
         */
        @CanIgnoreReturnValue
        public GpuCastAssert executesCorrectly(@Language("SQL") String sqlValueExpression)
        {
            CompiledExpression gpuExpression = requireCompiled();
            Page inputPage = buildInputPage(sqlValueExpression);

            Outcome cpu = runOnCpu(inputPage);
            Outcome gpu = runOnGpu(inputPage, gpuExpression);

            if (cpu instanceof Outcome.Failure(Throwable failure)) {
                throw new AssertionError(format(
                        "Expected cast %s -> %s of [%s] to succeed on CPU, but it failed: %s",
                        from,
                        to,
                        sqlValueExpression,
                        failure));
            }
            if (gpu instanceof Outcome.Failure(Throwable failure)) {
                throw new AssertionError(format(
                        "Expected cast %s -> %s of [%s] to succeed on GPU, but it failed: %s",
                        from,
                        to,
                        sqlValueExpression,
                        failure));
            }
            Object cpuValue = ((Outcome.Success) cpu).nativeValue();
            Object gpuValue = ((Outcome.Success) gpu).nativeValue();
            assertThat(gpuValue).describedAs("Cast %s -> %s of [%s]", from, to, sqlValueExpression)
                    .isEqualTo(cpuValue);
            return this;
        }

        /**
         * Asserts the cast of the given SQL-evaluated value fails on both GPU and CPU with the same error code.
         */
        @CanIgnoreReturnValue
        public GpuCastAssert failsCorrectly(@Language("SQL") String sqlValueExpression)
        {
            CompiledExpression gpuExpression = requireCompiled();
            Page inputPage = buildInputPage(sqlValueExpression);

            Outcome cpu = runOnCpu(inputPage);
            Outcome gpu = runOnGpu(inputPage, gpuExpression);

            if (cpu instanceof Outcome.Success(Object nativeValue)) {
                throw new AssertionError(format(
                        "Expected cast %s -> %s of [%s] to fail on CPU, but it produced: %s",
                        from,
                        to,
                        sqlValueExpression,
                        nativeValue));
            }
            if (gpu instanceof Outcome.Success(Object nativeValue)) {
                throw new AssertionError(format(
                        "Expected cast %s -> %s of [%s] to fail on GPU, but it produced: %s",
                        from,
                        to,
                        sqlValueExpression,
                        nativeValue));
            }
            ErrorCode cpuCode = errorCode(((Outcome.Failure) cpu).exception());
            ErrorCode gpuCode = errorCode(((Outcome.Failure) gpu).exception());
            boolean codesMatch = cpuCode != null && cpuCode.equals(gpuCode);
            // It's not always practical & efficient to distinguish between different cast failing modes.
            // INVALID_CAST_ARGUMENT is a generic "cannot cast" error, where NUMERIC_VALUE_OUT_OF_RANGE can be thought of as a sub-category
            boolean relaxedMatch = Set.copyOf(List.of(cpuCode, gpuCode)).equals(Set.of(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode(), INVALID_CAST_ARGUMENT.toErrorCode()));
            if (!codesMatch && !relaxedMatch) {
                throw new AssertionError(format(
                        "Cast %s -> %s of [%s]: CPU failed with %s, GPU failed with %s",
                        from,
                        to,
                        sqlValueExpression,
                        describeFailure(((Outcome.Failure) cpu).exception()),
                        describeFailure(((Outcome.Failure) gpu).exception())));
            }
            return this;
        }

        private CompiledExpression requireCompiled()
        {
            return compiled.get()
                    .orElseThrow(() -> new AssertionError(format("Cast %s -> %s does not compile on GPU", from, to)));
        }

        private Page buildInputPage(String sqlValueExpression)
        {
            Object nativeValue = evaluateSqlToNative(sqlValueExpression, from);
            return new Page(1, writeNativeValue(from, nativeValue));
        }

        private Outcome runOnCpu(Page inputPage)
        {
            try {
                Page page = getOnlyElement(executeWithCpu(List.of(inputPage), castExpression, layout));
                checkState(page.getChannelCount() == 1 && page.getPositionCount() == 1,
                        "Expected a one-column, one-row result; got %s columns and %s rows",
                        page.getChannelCount(),
                        page.getPositionCount());
                return new Outcome.Success(readNativeValue(to, page.getBlock(0), 0));
            }
            catch (Throwable t) {
                return new Outcome.Failure(t);
            }
        }

        private Outcome runOnGpu(Page inputPage, CompiledExpression gpuExpression)
        {
            try {
                List<Page> outputPages = executeGpuOperation(
                        List.of(inputPage),
                        List.of(from),
                        List.of(to),
                        copyToDevice -> new GpuProject(copyToDevice, List.of(new GpuProject.Projection.Gpu(gpuExpression))),
                        ImmutableSet.copyOf(gpuExpression.inputChannels().getInputChannels()));
                Page page = getOnlyElement(outputPages);
                checkState(page.getChannelCount() == 1 && page.getPositionCount() == 1,
                        "Expected a one-column, one-row result; got %s columns and %s rows",
                        page.getChannelCount(),
                        page.getPositionCount());
                return new Outcome.Success(readNativeValue(to, page.getBlock(0), 0));
            }
            catch (Throwable t) {
                return new Outcome.Failure(t);
            }
        }
    }

    private sealed interface Outcome
    {
        record Success(@Nullable Object nativeValue)
                implements Outcome {}

        record Failure(Throwable exception)
                implements Outcome
        {
            public Failure
            {
                requireNonNull(exception, "exception is null");
            }
        }
    }

    private static ErrorCode errorCode(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof TrinoException trinoException) {
                return trinoException.getErrorCode();
            }
            current = current.getCause();
        }
        return null;
    }

    private static String describeFailure(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof TrinoException trinoException) {
                return trinoException.getErrorCode() + ": " + trinoException.getMessage();
            }
            current = current.getCause();
        }
        return throwable.getClass().getName() + ": " + throwable.getMessage();
    }

    private @Nullable Object evaluateSqlToNative(String sqlExpression, Type expectedType)
    {
        List<Optional<?>> values = planTester.executePlan(session -> planTester.createPlan(session, "SELECT " + sqlExpression), new NativeValueOutput());
        Object value = getOnlyElement(values).orElse(null);
        return Primitives.wrap(expectedType.getJavaType()).cast(value);
    }

    /**
     * Captures the single value of a one-column, one-row query as a native internal representation,
     * as encoded in a {@link io.trino.spi.block.Block}.
     */
    private static class NativeValueOutput
            implements PlanTester.Output<List<Optional<?>>>
    {
        private final ImmutableList.Builder<Optional<?>> values = ImmutableList.builder();

        @Override
        public OutputFactory outputFactory()
        {
            return new PageConsumerOutputFactory(types -> {
                Type type = getOnlyElement(types);
                return page -> {
                    checkArgument(page.getChannelCount() == 1, "Expected exactly one column");
                    Block block = page.getBlock(0);
                    for (int position = 0; position < page.getPositionCount(); position++) {
                        values.add(Optional.ofNullable(readNativeValue(type, block, position)));
                    }
                };
            });
        }

        @Override
        public List<Optional<?>> result(List<String> columnNames)
        {
            return values.build();
        }
    }

    private List<Page> executeWithCpu(List<Page> inputPages, Expression expression, Map<Symbol, Integer> layout)
    {
        PageProcessor compiledProcessor = functionResolution.getExpressionCompiler().compilePageProcessor(
                        false,
                        true,
                        false,
                        true,
                        Optional.empty(),
                        Optional.empty(),
                        List.of(expression),
                        layout,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(InternalDynamicFilter.EMPTY);

        LocalMemoryContext context = newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName());
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        for (Page inputPage : inputPages) {
            Iterator<Optional<Page>> processed = compiledProcessor.process(fullConnectorSession, new DriverYieldSignal(), context, SourcePage.create(inputPage));
            stream(processed)
                    .flatMap(Optional::stream)
                    .forEachOrdered(outputPages::add);
        }
        return outputPages.build();
    }
}
