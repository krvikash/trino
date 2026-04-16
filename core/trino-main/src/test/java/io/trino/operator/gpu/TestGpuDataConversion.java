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
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for GPU conversion round trip: CPU → GPU → CPU.
 * Verifies that CopyToDevice + CopyToBlocks correctly preserve data.
 */
public class TestGpuDataConversion
{
    private final List<Type> testTypes = ImmutableList.<Type>builder()
            .add(BOOLEAN)
            .add(TINYINT)
            .add(SMALLINT)
            .add(INTEGER)
            .add(BIGINT)
            .add(REAL)
            .add(DOUBLE)
            .add(VARCHAR)
            .build();

    @Test
    public void testNoPages()
    {
        assertThatThrownBy(() -> executeRoundTrip(List.of(), testTypes, Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No columns to copy");
        assertThat(executeRoundTrip(List.of(), testTypes, Set.of(0)))
                .isEmpty();
        assertThat(executeRoundTrip(List.of(), testTypes, allChannels(testTypes.size())))
                .isEmpty();
    }

    @Test
    public void testEmptyPage()
    {
        List<Type> types = testTypes;
        List<Page> inputPages = createInputPages(List.of(0), NO_NULLS, types);
        List<Page> outputPages = executeRoundTrip(inputPages, types, allChannels(types.size()));
        assertThat(outputPages).isEmpty();
    }

    @Test
    public void testEmptyPages()
    {
        List<Type> types = testTypes;
        List<Page> inputPages = createInputPages(List.of(0, 0, 0, 0, 0, 0), NO_NULLS, types);
        List<Page> outputPages = executeRoundTrip(inputPages, types, allChannels(types.size()));
        assertThat(outputPages).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testSmallPage(NullsProvider nullsProvider)
    {
        List<Type> types = testTypes;
        List<Page> inputPages = createInputPages(List.of(16), nullsProvider, types);
        List<Page> outputPages = executeRoundTrip(inputPages, types, allChannels(types.size()));
        assertSameData(outputPages, inputPages, types);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testBigPage(NullsProvider nullsProvider)
    {
        List<Type> types = testTypes;
        List<Page> inputPages = createInputPages(List.of(213748), nullsProvider, types);
        List<Page> outputPages = executeRoundTrip(inputPages, types, allChannels(types.size()));
        assertSameData(outputPages, inputPages, types);
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testPages(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = randomInts(0, 100_000).limit(42).toList();
        List<Type> types = testTypes;
        List<Page> inputPages = createInputPages(positionsCounts, nullsProvider, types);
        List<Page> outputPages = executeRoundTrip(inputPages, types, allChannels(types.size()));
        assertSameData(outputPages, inputPages, types);
    }

    @Test
    public void testRealSpecialValues()
    {
        float[] values = {
                Float.NaN,
                Float.POSITIVE_INFINITY,
                Float.NEGATIVE_INFINITY,
                0.0f,
                -0.0f,
                Float.MIN_VALUE,
                Float.MAX_VALUE,
                -Float.MAX_VALUE,
                Float.intBitsToFloat(0x7F800001), // non-canonical NaN bit pattern
        };
        BlockBuilder builder = REAL.createBlockBuilder(null, values.length + 1);
        for (float v : values) {
            REAL.writeFloat(builder, v);
        }
        builder.appendNull();

        List<Page> output = executeRoundTrip(List.of(new Page(builder.build())), List.of(REAL), Set.of(0));
        int totalPositions = output.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(totalPositions).isEqualTo(values.length + 1);

        Streams.forEachPair(
                positions(output),
                IntStream.range(0, values.length + 1).boxed(),
                (actualPos, expectedIndex) -> {
                    Block block = actualPos.page.getBlock(0);
                    if (expectedIndex == values.length) {
                        assertThat(block.isNull(actualPos.position)).isTrue();
                        return;
                    }
                    long actualBits = (Long) readNativeValue(REAL, block, actualPos.position);
                    long expectedBits = Float.isNaN(values[expectedIndex])
                            ? 0x7FC00000L
                            : Float.floatToIntBits(values[expectedIndex]) & 0xFFFFFFFFL;
                    assertThat(actualBits & 0xFFFFFFFFL)
                            .as("REAL position %d", expectedIndex)
                            .isEqualTo(expectedBits);
                });
    }

    @Test
    public void testDoubleSpecialValues()
    {
        double[] values = {
                Double.NaN,
                Double.POSITIVE_INFINITY,
                Double.NEGATIVE_INFINITY,
                0.0,
                -0.0,
                Double.MIN_VALUE,
                Double.MAX_VALUE,
                -Double.MAX_VALUE,
                Double.longBitsToDouble(0x7FF0000000000001L), // non-canonical NaN bit pattern
        };
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, values.length + 1);
        for (double v : values) {
            DOUBLE.writeDouble(builder, v);
        }
        builder.appendNull();

        List<Page> output = executeRoundTrip(List.of(new Page(builder.build())), List.of(DOUBLE), Set.of(0));
        int totalPositions = output.stream().mapToInt(Page::getPositionCount).sum();
        assertThat(totalPositions).isEqualTo(values.length + 1);

        Streams.forEachPair(
                positions(output),
                IntStream.range(0, values.length + 1).boxed(),
                (actualPos, expectedIndex) -> {
                    Block block = actualPos.page.getBlock(0);
                    if (expectedIndex == values.length) {
                        assertThat(block.isNull(actualPos.position)).isTrue();
                        return;
                    }
                    long actualBits = Double.doubleToRawLongBits((Double) readNativeValue(DOUBLE, block, actualPos.position));
                    long expectedBits = Double.isNaN(values[expectedIndex])
                            ? 0x7FF8000000000000L
                            : Double.doubleToLongBits(values[expectedIndex]);
                    assertThat(actualBits)
                            .as("DOUBLE position %d", expectedIndex)
                            .isEqualTo(expectedBits);
                });
    }

    private List<Page> createInputPages(List<Integer> positionsCounts, NullsProvider nullsProvider, List<Type> types)
    {
        Block[][] pages = new Block[positionsCounts.size()][types.size()];
        for (int column = 0; column < types.size(); column++) {
            List<Block> blocks = createInputBlocks(positionsCounts, nullsProvider, types.get(column));
            checkState(blocks.size() == positionsCounts.size(), "blocks size mismatch");
            for (int pageNumber = 0; pageNumber < positionsCounts.size(); pageNumber++) {
                pages[pageNumber][column] = blocks.get(pageNumber);
            }
        }
        return Streams.zip(
                        positionsCounts.stream(),
                        Stream.of(pages),
                        Page::new)
                .collect(toImmutableList());
    }

    private List<Block> createInputBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider, Type type)
    {
        if (type == BOOLEAN) {
            return createBooleanBlocks(positionsCounts, nullsProvider);
        }
        if (type == TINYINT) {
            return createTinyintBlocks(positionsCounts, nullsProvider);
        }
        if (type == SMALLINT) {
            return createSmallintBlocks(positionsCounts, nullsProvider);
        }
        if (type == INTEGER) {
            return createIntegerBlocks(positionsCounts, nullsProvider);
        }
        if (type == BIGINT) {
            return createBigintBlocks(positionsCounts, nullsProvider);
        }
        if (type == REAL) {
            return createRealBlocks(positionsCounts, nullsProvider);
        }
        if (type == DOUBLE) {
            return createDoubleBlocks(positionsCounts, nullsProvider);
        }
        if (type == VARCHAR) {
            return createVarcharBlocks(positionsCounts, nullsProvider);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private List<Block> createBooleanBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = BOOLEAN.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            BOOLEAN.writeBoolean(builder, random.nextBoolean());
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private List<Block> createTinyintBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = TINYINT.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            TINYINT.writeLong(builder, random.nextInt(256) - 128); // -128 to 127
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private List<Block> createSmallintBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = SMALLINT.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            SMALLINT.writeLong(builder, random.nextInt(65536) - 32768); // -32768 to 32767
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private List<Block> createIntegerBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = INTEGER.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            INTEGER.writeLong(builder, random.nextInt());
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private List<Block> createBigintBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = BIGINT.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            BIGINT.writeLong(builder, random.nextLong());
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private List<Block> createRealBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = REAL.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            REAL.writeFloat(builder, random.nextFloat() * 1000 - 500); // -500 to 500
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private List<Block> createDoubleBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = DOUBLE.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            DOUBLE.writeDouble(builder, random.nextDouble() * 1000 - 500); // -500 to 500
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private List<Block> createVarcharBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Iterator<String> strings = generateInputStrings().iterator();
        return positionsCounts.stream()
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

    /**
     * Execute round trip: CPU → GPU → CPU without any operations.
     * Pipeline: BufferPages → CopyToDevice → CopyToBlocks → Pages
     */
    private List<Page> executeRoundTrip(List<Page> inputPages, List<Type> types, Set<Integer> channelsToTransfer)
    {
        Iterator<Page> input = inputPages.iterator();

        BufferPages bufferPages = new BufferPages();
        CopyToDevice copyToDevice = new CopyToDevice(bufferPages, types, channelsToTransfer);
        CopyToBlocks copyToBlocks = new CopyToBlocks(copyToDevice, types);
        GpuPageToPages gpuPageToPages = new GpuPageToPages();

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        while (true) {
            // Feed input pages
            if (!input.hasNext()) {
                bufferPages.noMoreInput();
            }
            else if (bufferPages.needsInput()) {
                bufferPages.addInput(input.next());
            }

            // Drain any completed pages
            gpuPageToPages.drain().forEachOrdered(outputPages::add);

            // Execute the pipeline
            @Own GpuOperation.Result result = copyToBlocks.execute();
            switch (result) {
                case GpuOperation.Blocked _ -> throw new UnsupportedOperationException("Blocked future not supported in test");
                case GpuOperation.Data(GpuPage gpuPage) -> {
                    try (gpuPage) {
                        gpuPageToPages.add(gpuPage);
                    }
                }
                case GpuOperation.Yielded() -> {
                    // Continue loop
                }
                case GpuOperation.Finished() -> {
                    checkState(gpuPageToPages.poll().isEmpty(), "gpuPageToPages should be drained");
                    return outputPages.build();
                }
            }
        }
    }

    /**
     * Generate infinite stream of test strings by cycling through testStrings
     * and mixing with random strings for variety.
     */
    private Stream<String> generateInputStrings()
    {
        List<String> testStrings = ImmutableList.<String>builder()
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

        Random random = new Random(42); // Fixed seed for reproducibility
        Stream<String> randomStrings = Stream.generate(() -> {
            int length = random.nextInt(51); // 0-50 chars
            char[] chars = new char[length];
            for (int i = 0; i < length; i++) {
                // Generate random valid Unicode characters (avoid surrogates)
                chars[i] = (char) random.nextInt(0, Character.MIN_SURROGATE - 1);
            }
            return new String(chars);
        });

        // Interleave test strings with random strings
        return Streams.zip(
                        Stream.generate(() -> testStrings).flatMap(List::stream),
                        randomStrings,
                        List::of)
                .flatMap(List::stream);
    }

    private void assertSameData(List<Page> actual, List<Page> expected, List<Type> types)
    {
        int actualRowCount = actual.stream().mapToInt(Page::getPositionCount).sum();
        int expectedRowCount = expected.stream().mapToInt(Page::getPositionCount).sum();

        assertThat(actualRowCount)
                .as("total row count after round trip")
                .isEqualTo(expectedRowCount);

        // Compare row by row
        Streams.forEachPair(
                positions(actual),
                positions(expected),
                (actualPos, expectedPos) -> {
                    List<Optional<Object>> actualValues = readValues(actualPos.page, actualPos.position, types);
                    List<Optional<Object>> expectedValues = readValues(expectedPos.page, expectedPos.position, types);

                    assertThat(actualValues)
                            .as("row %d values", actualPos.position)
                            .isEqualTo(expectedValues);
                });
    }

    private static Stream<Integer> randomInts(int minInclusive, int maxExclusive)
    {
        Random random = new Random(42); // Fixed seed for reproducibility
        return IntStream.generate(() -> random.nextInt(minInclusive, maxExclusive))
                .boxed();
    }

    private static Set<Integer> allChannels(int size)
    {
        return IntStream.range(0, size).boxed().collect(toImmutableSet());
    }

    private static List<Optional<Object>> readValues(Page page, int position, List<Type> types)
    {
        checkArgument(page.getChannelCount() == types.size(), "page channel count mismatch");
        return IntStream.range(0, types.size())
                .mapToObj(column -> Optional.ofNullable(readNativeValue(types.get(column), page.getBlock(column), position)))
                .collect(toImmutableList());
    }

    private static Stream<PagePosition> positions(List<Page> pages)
    {
        return pages.stream().flatMap(TestGpuDataConversion::positions);
    }

    private static Stream<PagePosition> positions(Page page)
    {
        return IntStream.range(0, page.getPositionCount())
                .mapToObj(i -> new PagePosition(page, i));
    }

    private record PagePosition(Page page, int position) {}
}
