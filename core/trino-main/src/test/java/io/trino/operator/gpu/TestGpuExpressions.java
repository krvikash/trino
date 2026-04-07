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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.trino.FullConnectorSession;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.gpu.borrow.Own;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuExpressionCompiler;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.planner.InternalDynamicFilter;
import io.trino.sql.relational.RowExpression;
import io.trino.testing.TestingSession;
import io.trino.type.LikePattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGpuExpressions
{
    private static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));

    private final TestingFunctionResolution functionResolution = new TestingFunctionResolution();
    private final GpuExpressionCompiler gpuCompiler = new GpuExpressionCompiler();

    /**
     * Useful test strings, including interesting inputs and patterns for LIKE testing.
     */
    private final List<String> testStrings = ImmutableList.<String>builder()
            .add("test1", "other", "test2", "nothing", "testing", "%test%")
            .add("a", "xyz", "ab", "z", "yz", "abcd", "", "abcdefg", "xabc", "xyxw", "xaxxxbx", "abcdefghij")
            .add("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .add("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab")
            .add("aabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabb")
            .add("aaaabbbbaaaabbbbaaaabbbb")
            .add("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .add("aaaabbbbaaaabbbbaaaa", "aaaabbbbaaaabbbbcccc")
            .add("abababababacabababa", "bbbbbbbbxax", "bbbxxxxaz")
            .add("a".repeat(20) + "b".repeat(20) + "a".repeat(20) + "b".repeat(20) + "the quick brown fox jumps over the lazy dog")
            .add("ababaa", "papaya", "papapaya", "papapapaya", "papapapapaya", "papapapapapaya")
            .add("xyza1234567890123456")
            .add("%", "_", "-", "xxxxx_xxxxx")
            .add("__", "a%", "a_", "%a", "%z", "_z", "_%", "_a%", "_ab_", "_a%b_", "_%_%_%_%")
            .add("%a%a%a%a%a%a%", "%a%b%a%b%a%b%", "%aaaa%bbbb%aaaa%bbbb%aaaa%bbbb%")
            .add("%aaaaaaaaaaaaaaaaaaaaaaaaaa%", "%aab%bba%aab%bba%", "%abaca%")
            .add("%bcccccccca%", "%bbxxxxxa%", "%aaaaaaxaaaaaa%", "%abaaa%", "%paya%")
            .add("%a________________", "-%", "-_", "--", "%$_%")
            .add("Łania szła piękną łąką pod Warszawą")
            .add("ワルシャワ近郊の美しい草原を雌鹿が歩いていた。")
            .add("Слава Україні")
            .build();

    private final int stringChannel = 0;

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLikeSmall(NullsProvider nullsProvider)
    {
        testLike(List.of(64), nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLikeMany(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = Stream.generate(() -> 10_000).limit(42).toList();
        testLike(positionsCounts, nullsProvider);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testLikeRandomPages(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = randomInts(0, 200_00).limit(42).toList();
        testLike(positionsCounts, nullsProvider);
    }

    private void testLike(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        List<Page> inputPages = createVarcharBlocks(
                positionsCounts.iterator(),
                nullsProvider).stream()
                .map(Page::new)
                .collect(toImmutableList());

        List<Optional<Character>> escapes = List.of(Optional.of('\\'), Optional.of('$'), Optional.empty());
        for (String pattern : testStrings) {
            for (Optional<Character> escape : escapes) {
                int channel = 0;
                RowExpression rowExpression = createLikeExpression(stringChannel, pattern, escape);
                CompiledExpression gpuExpression = gpuCompiler.compileExpression(rowExpression).orElseThrow(() -> new AssertionError("GPU expression compile failed"));
                assertThat(gpuExpression.inputChannels().getInputChannels()).containsExactly(channel);
                List<Page> gpuResults = executeWithGpu(inputPages, rowExpression, gpuExpression);
                List<Page> cpuResults = executeWithCpu(inputPages, rowExpression);
                assertSameData(gpuResults, cpuResults, List.of(rowExpression.type()));
            }
        }
    }

    @Test
    public void testBooleanConstant()
    {
        testConstant(constant(true, BOOLEAN), BOOLEAN);
        testConstant(constant(false, BOOLEAN), BOOLEAN);
        testConstant(constant(null, BOOLEAN), BOOLEAN);
    }

    @Test
    public void testTinyintConstant()
    {
        testConstant(constant(42L, TINYINT), TINYINT);
        testConstant(constant(-1L, TINYINT), TINYINT);
        testConstant(constant(0L, TINYINT), TINYINT);
        testConstant(constant(null, TINYINT), TINYINT);
    }

    @Test
    public void testSmallintConstant()
    {
        testConstant(constant(1234L, SMALLINT), SMALLINT);
        testConstant(constant(-5678L, SMALLINT), SMALLINT);
        testConstant(constant(null, SMALLINT), SMALLINT);
    }

    @Test
    public void testIntegerConstant()
    {
        testConstant(constant(123456L, INTEGER), INTEGER);
        testConstant(constant(-789012L, INTEGER), INTEGER);
        testConstant(constant(0L, INTEGER), INTEGER);
        testConstant(constant(null, INTEGER), INTEGER);
    }

    @Test
    public void testBigintConstant()
    {
        testConstant(constant(1234567890123L, BIGINT), BIGINT);
        testConstant(constant(-9876543210L, BIGINT), BIGINT);
        testConstant(constant(0L, BIGINT), BIGINT);
        testConstant(constant(null, BIGINT), BIGINT);
    }

    @Test
    public void testRealConstant()
    {
        testConstant(constant((long) Float.floatToIntBits(3.14f), REAL), REAL);
        testConstant(constant((long) Float.floatToIntBits(-2.5f), REAL), REAL);
        testConstant(constant((long) Float.floatToIntBits(0.0f), REAL), REAL);
        testConstant(constant((long) Float.floatToIntBits(Float.POSITIVE_INFINITY), REAL), REAL);
        testConstant(constant((long) Float.floatToIntBits(Float.NEGATIVE_INFINITY), REAL), REAL);
        testConstant(constant((long) Float.floatToIntBits(Float.NaN), REAL), REAL);
        testConstant(constant(null, REAL), REAL);
    }

    @Test
    public void testDoubleConstant()
    {
        testConstant(constant(3.14159265359, DOUBLE), DOUBLE);
        testConstant(constant(-2.71828, DOUBLE), DOUBLE);
        testConstant(constant(0.0, DOUBLE), DOUBLE);
        testConstant(constant(Double.POSITIVE_INFINITY, DOUBLE), DOUBLE);
        testConstant(constant(Double.NEGATIVE_INFINITY, DOUBLE), DOUBLE);
        testConstant(constant(Double.NaN, DOUBLE), DOUBLE);
        testConstant(constant(null, DOUBLE), DOUBLE);
    }

    @Test
    public void testVarcharConstant()
    {
        testConstant(constant(Slices.utf8Slice("hello"), VARCHAR), VARCHAR);
        testConstant(constant(Slices.utf8Slice(""), VARCHAR), VARCHAR);
        testConstant(constant(Slices.utf8Slice("Łania szła piękną łąką pod Warszawą"), VARCHAR), VARCHAR);
        testConstant(constant(null, VARCHAR), VARCHAR);
    }

    private void testConstant(RowExpression constantExpression, Type expectedType)
    {
        List<Page> inputPages = createVarcharBlocks(
                List.of(64).iterator(),
                NullsProvider.NO_NULLS).stream()
                .map(Page::new)
                .collect(toImmutableList());

        CompiledExpression gpuExpression = gpuCompiler.compileExpression(constantExpression)
                .orElseThrow(() -> new AssertionError("GPU expression compile failed for: " + constantExpression));
        assertThat(gpuExpression.inputChannels().getInputChannels()).isEmpty();

        List<Page> gpuResults = executeWithGpu(inputPages, constantExpression, gpuExpression);
        List<Page> cpuResults = executeWithCpu(inputPages, constantExpression);
        assertSameData(gpuResults, cpuResults, List.of(expectedType));
    }

    private List<Page> executeWithGpu(List<Page> inputPages, RowExpression rowExpression, CompiledExpression gpuExpression)
    {
        Iterator<Page> input = inputPages.iterator();

        BufferPages bufferPages = new BufferPages();
        CopyToDevice copyToDevice = new CopyToDevice(
                bufferPages,
                List.of(VARCHAR),
                Set.of(stringChannel));
        GpuProject gpuFilter = new GpuProject(copyToDevice, List.of(new GpuProject.Projection.Gpu(gpuExpression)));
        CopyToBlocks copyToBlocks = new CopyToBlocks(gpuFilter, List.of(rowExpression.type()));
        GpuPageToPages gpuPageToPages = new GpuPageToPages();

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        while (true) {
            if (!input.hasNext()) {
                bufferPages.noMoreInput();
            }
            else if (bufferPages.needsInput()) {
                bufferPages.addInput(input.next());
            }

            gpuPageToPages.drain().forEachOrdered(outputPages::add);

            @Own GpuOperation.Result result = copyToBlocks.execute();
            switch (result) {
                case GpuOperation.Blocked _ -> throw new UnsupportedOperationException("Unsupported blocked future, what shall I do?");
                case GpuOperation.Data(GpuPage gpuPage) -> {
                    try (gpuPage) {
                        gpuPageToPages.add(gpuPage);
                    }
                }
                case GpuOperation.Yielded() -> {
                    // continue
                }
                case GpuOperation.Finished() -> {
                    checkState(gpuPageToPages.poll().isEmpty(), "gpuPageToPages should be drained at this point");
                    return outputPages.build();
                }
            }
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
            Iterator<Optional<Page>> processed = compiledProcessor.process(FULL_CONNECTOR_SESSION, new DriverYieldSignal(), context, SourcePage.create(inputPage));
            stream(processed)
                    .flatMap(Optional::stream)
                    .forEachOrdered(outputPages::add);
        }
        return outputPages.build();
    }

    private RowExpression createLikeExpression(int channel, String pattern, Optional<Character> escape)
    {
        return call(
                functionResolution.resolveFunction("$like", fromTypes(VARCHAR, LIKE_PATTERN)),
                field(channel, VARCHAR),
                constant(LikePattern.compile(pattern, escape), LIKE_PATTERN));
    }

    private List<Block> createVarcharBlocks(Iterator<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Iterator<String> strings = generateInputStrings().iterator();
        return stream(positionsCounts)
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    VariableWidthBlockBuilder builder = new VariableWidthBlockBuilder(null, positionsCount, positionsCount * 10);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            builder.writeEntry(Slices.utf8Slice(strings.next()));
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private Stream<String> generateInputStrings()
    {
        Random random = new Random(42);
        Stream<String> randomStrings = Stream.generate(() -> {
            int length = random.nextInt(21);
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                chars[i] = (char) random.nextInt(0, Character.MIN_SURROGATE - 1);
            }
            return new String(chars);
        });

        return Streams.zip(
                        Stream.generate(() -> testStrings).flatMap(List::stream),
                        randomStrings,
                        List::of)
                .flatMap(List::stream);
    }

    private void assertSameData(List<Page> actual, List<Page> expected, List<Type> types)
    {
        assertThat(actual.stream().mapToInt(Page::getPositionCount).sum()).as("actual position count (sum over all returned pages)")
                .isEqualTo(expected.stream().mapToInt(Page::getPositionCount).sum());

        Streams.forEachPair(
                positions(actual),
                positions(expected),
                (left, right) -> {
                    assertThat(readValues(left.page, left.position, types))
                            .isEqualTo(readValues(right.page, right.position, types));
                });
    }

    private static Stream<Integer> randomInts(int minInclusive, int maxExclusive)
    {
        Random random = new Random(42); // Fixed seed for reproducibility
        return IntStream.generate(() -> random.nextInt(minInclusive, maxExclusive))
                .boxed();
    }

    private static List<Optional<Object>> readValues(Page page, int position, List<Type> types)
    {
        checkArgument(page.getChannelCount() == types.size());
        return IntStream.range(0, types.size())
                .mapToObj(column -> Optional.ofNullable(readNativeValue(types.get(column), page.getBlock(column), position)))
                .collect(toImmutableList());
    }

    private static Stream<PagePosition> positions(List<Page> pages)
    {
        return pages.stream().flatMap(TestGpuExpressions::positions);
    }

    private static Stream<PagePosition> positions(Page page)
    {
        return IntStream.range(0, page.getPositionCount())
                .mapToObj(i -> new PagePosition(page, i));
    }

    private record PagePosition(Page page, int position) {}
}
