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
import io.trino.operator.gpu.expression.GpuExpressionCompiler;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.ErrorCode;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.TrinoNumber.Infinity;
import io.trino.spi.type.TrinoNumber.NotANumber;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.planner.InternalDynamicFilter;
import io.trino.sql.relational.RowExpression;
import io.trino.testing.PageConsumerOperator.PageConsumerOutputFactory;
import io.trino.testing.PlanTester;
import io.trino.type.NumberOperators;
import jakarta.annotation.Nullable;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AssertProvider;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Streams.stream;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
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
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.field;
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
    private GpuExpressionCompiler gpuCompiler;
    private FullConnectorSession fullConnectorSession;

    @BeforeAll
    void setUp()
    {
        Session session = testSessionBuilder().build();
        planTester = PlanTester.create(session);
        functionResolution = new TestingFunctionResolution(planTester.getTransactionManager(), planTester.getPlannerContext());
        gpuCompiler = new GpuExpressionCompiler();
        fullConnectorSession = new FullConnectorSession(session, session.getIdentity().toConnectorIdentity());
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
    void testNumericCastCorrectnessSmoke()
    {
        List<Type> testedTypes = List.of(
                TINYINT,
                SMALLINT,
                INTEGER,
                BIGINT,
                REAL,
                DOUBLE,
                createDecimalType(1, 0),
                createDecimalType(3, 0),
                createDecimalType(13, 0),
                createDecimalType(13, 2),
                createDecimalType(27, 0),
                createDecimalType(27, 5),
                createDecimalType(38),
                NUMBER);

        List<TrinoNumber> testedNumbers = numericValuesToTest();
        List<String> numberLiterals = testedNumbers.stream()
                .map(value -> "NUMBER '%s'".formatted(NumberOperators.castToVarchar(VarcharType.UNBOUNDED_LENGTH, value).toStringUtf8()))
                .toList();

        for (Type from : testedTypes) {
            List<String> possibleValuesInFromType = tryCastToAndFilter(from, numberLiterals);
            verify(!possibleValuesInFromType.isEmpty());

            for (Type to : testedTypes) {
                GpuCastAssert assertion = assertThat(gpuCast(from, to));
                try {
                    assertion.isNotSupported();
                    continue;
                }
                catch (AssertionError e) {
                    if (!nullToEmpty(e.getMessage()).matches("Expected cast .* to be unsupported on GPU, but it compiled")) {
                        throw e;
                    }
                }

                for (String value : possibleValuesInFromType) {
                    assertion.executesCorrectly(value);
                }
            }
        }
    }

    private static List<TrinoNumber> numericValuesToTest()
    {
        List<Long> initial = List.of(
                0L,
                -1L, 1L,
                (long) Byte.MIN_VALUE, (long) Byte.MAX_VALUE,
                (long) Short.MIN_VALUE, (long) Short.MAX_VALUE,
                (long) Integer.MIN_VALUE, (long) Integer.MAX_VALUE,
                Long.MIN_VALUE, Long.MAX_VALUE);
        List<BigDecimal> offsets = Stream.of("0", "1", "-1", "0.33", "0.5", "0.66", "-0.33", "-0.5", "-0.66")
                .map(BigDecimal::new)
                .toList();
        List<TrinoNumber> testedNumbers = new ArrayList<>();
        for (long value : initial) {
            BigDecimal asBigDecimal = BigDecimal.valueOf(value);
            for (BigDecimal offset : offsets) {
                testedNumbers.add(TrinoNumber.from(asBigDecimal.add(offset)));
            }
        }
        testedNumbers.add(TrinoNumber.from(BigDecimal.valueOf(Math.PI)));
        testedNumbers.add(TrinoNumber.from(new Infinity(false)));
        testedNumbers.add(TrinoNumber.from(new Infinity(true)));
        testedNumbers.add(TrinoNumber.from(new NotANumber()));
        return testedNumbers;
    }

    private List<String> tryCastToAndFilter(Type toType, List<String> literals)
    {
        List<String> expressions = new ArrayList<>();
        for (String literal : literals) {
            String expression = "CAST(%s AS %s)".formatted(literal, toType.getDisplayName());
            try {
                planTester.executeStatement("VALUES ROW(%s)".formatted(expression));
            }
            catch (TrinoException e) {
                if (e.getErrorCode().equals(NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode()) || e.getErrorCode().equals(INVALID_CAST_ARGUMENT.toErrorCode())) {
                    continue;
                }
                throw e;
            }
            expressions.add(expression);
        }
        return expressions;
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
        // DECIMAL(27,5) and NUMBER are not yet supported on GPU
        assertThat(gpuCast(TINYINT, DECIMAL_27_5)).isNotSupported();
        assertThat(gpuCast(TINYINT, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromSmallint()
    {
        String[] values = {
                "SMALLINT '-1'",
                "SMALLINT '0'",
                "SMALLINT '1'",
                "SMALLINT '-32768'",
                "SMALLINT '32767'",
                "CAST(NULL AS SMALLINT)",
        };
        assertThat(gpuCast(SMALLINT, TINYINT)).isNotSupported();
        assertCastSucceedsForAll(SMALLINT, SMALLINT, values);
        assertCastSucceedsForAll(SMALLINT, INTEGER, values);
        assertCastSucceedsForAll(SMALLINT, BIGINT, values);
        assertCastSucceedsForAll(SMALLINT, REAL, values);
        assertCastSucceedsForAll(SMALLINT, DOUBLE, values);
        assertCastSucceedsForAll(SMALLINT, DECIMAL_13_2, values);
        assertThat(gpuCast(SMALLINT, DECIMAL_27_5)).isNotSupported();
        assertThat(gpuCast(SMALLINT, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromInteger()
    {
        String[] values = {
                "INTEGER '-1'",
                "INTEGER '0'",
                "INTEGER '1'",
                "INTEGER '-2147483648'",
                "INTEGER '2147483647'",
                "CAST(NULL AS INTEGER)",
        };
        assertThat(gpuCast(INTEGER, TINYINT)).isNotSupported();
        assertThat(gpuCast(INTEGER, SMALLINT)).isNotSupported();
        assertCastSucceedsForAll(INTEGER, INTEGER, values);
        assertCastSucceedsForAll(INTEGER, BIGINT, values);
        assertCastSucceedsForAll(INTEGER, REAL, values);
        assertCastSucceedsForAll(INTEGER, DOUBLE, values);
        assertCastSucceedsForAll(INTEGER, DECIMAL_13_2, values);
        assertThat(gpuCast(INTEGER, DECIMAL_27_5)).isNotSupported();
        assertThat(gpuCast(INTEGER, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromBigint()
    {
        String[] values = {
                "BIGINT '-1'",
                "BIGINT '0'",
                "BIGINT '1'",
                "BIGINT '-9223372036854775808'",
                "BIGINT '9223372036854775807'",
                "CAST(NULL AS BIGINT)",
        };
        assertThat(gpuCast(BIGINT, TINYINT)).isNotSupported();
        assertThat(gpuCast(BIGINT, SMALLINT)).isNotSupported();
        assertThat(gpuCast(BIGINT, INTEGER)).isNotSupported();
        // BIGINT (19 digits) does not fit DECIMAL(13,2) integer range (11 digits)
        assertThat(gpuCast(BIGINT, DECIMAL_13_2)).isNotSupported();
        assertCastSucceedsForAll(BIGINT, BIGINT, values);
        assertCastSucceedsForAll(BIGINT, REAL, values);
        assertCastSucceedsForAll(BIGINT, DOUBLE, values);
        assertThat(gpuCast(BIGINT, DECIMAL_27_5)).isNotSupported();
        assertThat(gpuCast(BIGINT, NUMBER)).isNotSupported();
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
        assertThat(gpuCast(DECIMAL_13_2, DECIMAL_27_5)).isNotSupported();
        assertThat(gpuCast(DECIMAL_13_2, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromDecimal27()
    {
        assertThat(gpuCast(DECIMAL_27_5, TINYINT)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, SMALLINT)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, INTEGER)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, BIGINT)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, REAL)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, DOUBLE)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, DECIMAL_13_2)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, DECIMAL_27_5)).isNotSupported();
        assertThat(gpuCast(DECIMAL_27_5, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromReal()
    {
        String[] values = {
                "REAL '-1.0'",
                "REAL '0.0'",
                "REAL '1.0'",
                "REAL '-3.4028235E38'",
                "REAL '3.4028235E38'",
                "CAST(NULL AS REAL)",
        };
        assertThat(gpuCast(REAL, TINYINT)).isNotSupported();
        assertThat(gpuCast(REAL, SMALLINT)).isNotSupported();
        assertThat(gpuCast(REAL, INTEGER)).isNotSupported();
        assertThat(gpuCast(REAL, BIGINT)).isNotSupported();
        assertThat(gpuCast(REAL, DECIMAL_13_2)).isNotSupported();
        assertCastSucceedsForAll(REAL, REAL, values);
        assertCastSucceedsForAll(REAL, DOUBLE, values);
        assertThat(gpuCast(REAL, DECIMAL_27_5)).isNotSupported();
        assertThat(gpuCast(REAL, NUMBER)).isNotSupported();
    }

    @Test
    void testCastFromDouble()
    {
        String[] values = {
                "DOUBLE '-1.0'",
                "DOUBLE '0.0'",
                "DOUBLE '1.0'",
                "DOUBLE '-1.7976931348623157E308'",
                "DOUBLE '1.7976931348623157E308'",
                "CAST(NULL AS DOUBLE)",
        };
        assertThat(gpuCast(DOUBLE, TINYINT)).isNotSupported();
        assertThat(gpuCast(DOUBLE, SMALLINT)).isNotSupported();
        assertThat(gpuCast(DOUBLE, INTEGER)).isNotSupported();
        assertThat(gpuCast(DOUBLE, BIGINT)).isNotSupported();
        assertThat(gpuCast(DOUBLE, REAL)).isNotSupported();
        assertThat(gpuCast(DOUBLE, DECIMAL_13_2)).isNotSupported();
        assertCastSucceedsForAll(DOUBLE, DOUBLE, values);
        assertThat(gpuCast(DOUBLE, DECIMAL_27_5)).isNotSupported();
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
        private final RowExpression castExpression;
        private final Supplier<Optional<CompiledExpression>> compiled;

        GpuCastAssert(Type from, Type to)
        {
            super(from + " -> " + to, GpuCastAssert.class);
            this.from = from;
            this.to = to;
            this.castExpression = call(functionResolution.getCoercion(from, to), field(0, from));
            this.compiled = Suppliers.memoize(() -> gpuCompiler.compileExpression(castExpression));
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
                        from, to, sqlValueExpression, failure));
            }
            if (gpu instanceof Outcome.Failure(Throwable failure)) {
                throw new AssertionError(format(
                        "Expected cast %s -> %s of [%s] to succeed on GPU, but it failed: %s",
                        from, to, sqlValueExpression, failure));
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
                        from, to, sqlValueExpression, nativeValue));
            }
            if (gpu instanceof Outcome.Success(Object nativeValue)) {
                throw new AssertionError(format(
                        "Expected cast %s -> %s of [%s] to fail on GPU, but it produced: %s",
                        from, to, sqlValueExpression, nativeValue));
            }
            ErrorCode cpuCode = errorCode(((Outcome.Failure) cpu).exception());
            ErrorCode gpuCode = errorCode(((Outcome.Failure) gpu).exception());
            if (cpuCode == null || !cpuCode.equals(gpuCode)) {
                throw new AssertionError(format(
                        "Cast %s -> %s of [%s]: CPU failed with %s, GPU failed with %s",
                        from, to, sqlValueExpression,
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
                Page page = getOnlyElement(executeWithCpu(List.of(inputPage), castExpression));
                checkState(page.getChannelCount() == 1 && page.getPositionCount() == 1,
                        "Expected a one-column, one-row result; got %s columns and %s rows",
                        page.getChannelCount(), page.getPositionCount());
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
                        page.getChannelCount(), page.getPositionCount());
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

    private List<Page> executeWithCpu(List<Page> inputPages, RowExpression expression)
    {
        PageProcessor compiledProcessor = functionResolution.getExpressionCompiler().compilePageProcessor(
                        false,
                        true,
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        List.of(expression),
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
