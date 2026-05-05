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

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.HostColumnVector;
import com.google.common.collect.Streams;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.Int128ArrayBlock;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.operator.gpu.GpuTestUtils.TESTED_GPU_TYPES;
import static io.trino.operator.gpu.GpuTestUtils.assertSameDataInOrder;
import static io.trino.operator.gpu.GpuTestUtils.createBlocks;
import static io.trino.operator.gpu.GpuTestUtils.executeGpuOperation;
import static io.trino.operator.gpu.GpuTestUtils.maybeSetGpuMemoryPoolForTests;
import static io.trino.operator.gpu.GpuTestUtils.positions;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.sql.gen.TestColumnarFilters.NullsProvider.NO_NULLS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for GPU conversion round trip: CPU → GPU → CPU.
 * Verifies that CopyToDevice + CopyToBlocks correctly preserve data.
 */
public class TestGpuDataConversion
{
    @BeforeAll
    public static void maybeSetGpuMemoryPool()
    {
        maybeSetGpuMemoryPoolForTests();
    }

    @Test
    public void testNoPages()
    {
        assertThatThrownBy(() -> executeRoundTrip(List.of(), TESTED_GPU_TYPES, Set.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No columns to copy");
        assertThat(executeRoundTrip(List.of(), TESTED_GPU_TYPES, Set.of(0)))
                .isEmpty();
        assertThat(executeRoundTrip(List.of(), TESTED_GPU_TYPES, allChannels(TESTED_GPU_TYPES.size())))
                .isEmpty();
    }

    /**
     * Long DECIMAL support copies Trino {@link Int128ArrayBlock} {@code (high, low)} pairs into
     * cuDF's DECIMAL128 little-endian layout (low 64 bits first). Round-trip tests would pass even
     * if both directions agreed on a wrong layout, so verify the Trino-to-GPU side independently
     * against cuDF's own {@link HostColumnVector#getBigDecimal} reader.
     */
    @Test
    public void testLongDecimalLayoutMatchesCudf()
    {
        DecimalType decimalType = createDecimalType(38, 10);
        int scale = decimalType.getScale();
        List<BigInteger> expected = List.of(
                BigInteger.ZERO,
                BigInteger.ONE,
                BigInteger.ONE.negate(),
                BigInteger.valueOf(Long.MAX_VALUE),
                BigInteger.valueOf(Long.MIN_VALUE),
                Int128.MAX_VALUE.toBigInteger(),
                Int128.MIN_VALUE.toBigInteger(),
                new BigInteger("1234567890123456789012345678"));
        long[] highLow = new long[expected.size() * 2];
        for (int i = 0; i < expected.size(); i++) {
            Int128 value = Int128.valueOf(expected.get(i));
            highLow[2 * i] = value.getHigh();
            highLow[2 * i + 1] = value.getLow();
        }
        Int128ArrayBlock block = new Int128ArrayBlock(expected.size(), Optional.empty(), highLow);

        GpuTypeConversion.GpuTypeMapping mapping = GpuTypeConversion.toGpuMapping(decimalType).orElseThrow();
        try (Blocks blocks = new Blocks(List.of(block));
                ColumnVector column = mapping.toColumn().copyToDevice(blocks);
                HostColumnVector host = column.copyToHost()) {
            for (int i = 0; i < expected.size(); i++) {
                BigDecimal actual = host.getBigDecimal(i);
                assertThat(actual.unscaledValue()).isEqualTo(expected.get(i));
                assertThat(actual.scale()).isEqualTo(scale);
            }
        }
    }

    @Test
    public void testEmptyPage()
    {
        List<Type> types = TESTED_GPU_TYPES;
        List<Page> inputPages = createInputPages(List.of(0), NO_NULLS, types);
        List<Page> outputPages = executeRoundTrip(inputPages, types, allChannels(types.size()));
        assertThat(outputPages).isEmpty();
    }

    @Test
    public void testEmptyPages()
    {
        List<Type> types = TESTED_GPU_TYPES;
        List<Page> inputPages = createInputPages(List.of(0, 0, 0, 0, 0, 0), NO_NULLS, types);
        List<Page> outputPages = executeRoundTrip(inputPages, types, allChannels(types.size()));
        assertThat(outputPages).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testSmallPage(NullsProvider nullsProvider)
    {
        for (Type type : TESTED_GPU_TYPES) {
            List<Type> types = List.of(type);
            List<Page> inputPages = createInputPages(List.of(16), nullsProvider, types);
            List<Page> outputPages = executeRoundTrip(inputPages, types, Set.of(0));
            assertSameDataInOrder(outputPages, inputPages, types);
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testBigPage(NullsProvider nullsProvider)
    {
        for (Type type : TESTED_GPU_TYPES) {
            List<Type> types = List.of(type);
            List<Page> inputPages = createInputPages(List.of(213748), nullsProvider, types);
            List<Page> outputPages = executeRoundTrip(inputPages, types, Set.of(0));
            assertSameDataInOrder(outputPages, inputPages, types);
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testPages(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = randomInts(0, 100_000).limit(42).toList();
        for (Type type : TESTED_GPU_TYPES) {
            List<Type> types = List.of(type);
            List<Page> inputPages = createInputPages(positionsCounts, nullsProvider, types);
            List<Page> outputPages = executeRoundTrip(inputPages, types, Set.of(0));
            assertSameDataInOrder(outputPages, inputPages, types);
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testRlePages(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = randomInts(0, 10_000).limit(20).toList();
        for (Type type : TESTED_GPU_TYPES) {
            List<Type> types = List.of(type);
            List<Page> inputPages = toRlePages(createInputPages(positionsCounts, nullsProvider, types));
            List<Page> outputPages = executeRoundTrip(inputPages, types, Set.of(0));
            assertSameDataInOrder(outputPages, inputPages, types);
        }
    }

    @ParameterizedTest
    @EnumSource(NullsProvider.class)
    public void testDictionaryPages(NullsProvider nullsProvider)
    {
        List<Integer> positionsCounts = randomInts(0, 10_000).limit(20).toList();
        for (Type type : TESTED_GPU_TYPES) {
            List<Type> types = List.of(type);
            List<Page> inputPages = toDictionaryPages(createInputPages(positionsCounts, nullsProvider, types));
            List<Page> outputPages = executeRoundTrip(inputPages, types, Set.of(0));
            assertSameDataInOrder(outputPages, inputPages, types);
        }
    }

    private static List<Page> toRlePages(List<Page> pages)
    {
        return pages.stream()
                .map(page -> {
                    Block[] blocks = new Block[page.getChannelCount()];
                    for (int channel = 0; channel < page.getChannelCount(); channel++) {
                        Block block = page.getBlock(channel);
                        blocks[channel] = block.getPositionCount() == 0
                                ? block
                                : RunLengthEncodedBlock.create(block.getSingleValueBlock(0), block.getPositionCount());
                    }
                    return new Page(page.getPositionCount(), blocks);
                })
                .collect(toImmutableList());
    }

    private static List<Page> toDictionaryPages(List<Page> pages)
    {
        Random random = new Random(42);
        return pages.stream()
                .map(page -> {
                    int positionCount = page.getPositionCount();
                    Block[] blocks = new Block[page.getChannelCount()];
                    if (positionCount == 0) {
                        for (int channel = 0; channel < page.getChannelCount(); channel++) {
                            blocks[channel] = page.getBlock(channel);
                        }
                    }
                    else {
                        int[] ids = new int[positionCount];
                        for (int i = 0; i < positionCount; i++) {
                            ids[i] = random.nextInt(positionCount);
                        }
                        for (int channel = 0; channel < page.getChannelCount(); channel++) {
                            blocks[channel] = DictionaryBlock.create(positionCount, page.getBlock(channel), ids);
                        }
                    }
                    return new Page(positionCount, blocks);
                })
                .collect(toImmutableList());
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
                    Block block = actualPos.page().getBlock(0);
                    if (expectedIndex == values.length) {
                        assertThat(block.isNull(actualPos.position())).isTrue();
                        return;
                    }
                    long actualBits = (Long) readNativeValue(REAL, block, actualPos.position());
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
                    Block block = actualPos.page().getBlock(0);
                    if (expectedIndex == values.length) {
                        assertThat(block.isNull(actualPos.position())).isTrue();
                        return;
                    }
                    long actualBits = Double.doubleToRawLongBits((Double) readNativeValue(DOUBLE, block, actualPos.position()));
                    long expectedBits = Double.isNaN(values[expectedIndex])
                            ? 0x7FF8000000000000L
                            : Double.doubleToLongBits(values[expectedIndex]);
                    assertThat(actualBits)
                            .as("DOUBLE position %d", expectedIndex)
                            .isEqualTo(expectedBits);
                });
    }

    @Test
    public void testUnsupportedTimestampPrecisions()
    {
        for (int precision : List.of(1, 2, 4, 5)) {
            assertThat(GpuTypeConversion.toGpuMapping(TimestampType.createTimestampType(precision)))
                    .as("short timestamp precision %d", precision)
                    .isEmpty();
        }
        for (int precision : List.of(7, 9, 12)) {
            assertThat(GpuTypeConversion.toGpuMapping(TimestampType.createTimestampType(precision)))
                    .as("long timestamp precision %d", precision)
                    .isEmpty();
        }
    }

    @Test
    public void testDecimalTypeMappings()
    {
        // Short decimals (precision ≤ 18) ride DECIMAL64; long decimals ride DECIMAL128.
        assertThat(GpuTypeConversion.toDType(createDecimalType(1, 0))).hasValue(DType.create(DType.DTypeEnum.DECIMAL64, 0));
        assertThat(GpuTypeConversion.toDType(createDecimalType(18, 6))).hasValue(DType.create(DType.DTypeEnum.DECIMAL64, -6));
        assertThat(GpuTypeConversion.toDType(createDecimalType(19, 5))).hasValue(DType.create(DType.DTypeEnum.DECIMAL128, -5));
        assertThat(GpuTypeConversion.toDType(createDecimalType(38, 10))).hasValue(DType.create(DType.DTypeEnum.DECIMAL128, -10));
    }

    @Test
    public void testDecimalBoundaries()
    {
        for (DecimalType type : List.of(
                createDecimalType(1, 0),
                createDecimalType(1, 1),
                createDecimalType(5, 5),
                createDecimalType(18, 0),
                createDecimalType(18, 18),
                createDecimalType(19, 0),
                createDecimalType(27, 4),
                createDecimalType(38, 0),
                createDecimalType(38, 38))) {
            BigInteger maxUnscaled = BigInteger.TEN.pow(type.getPrecision()).subtract(BigInteger.ONE);
            List<BigInteger> values = List.of(
                    maxUnscaled.negate(),
                    BigInteger.ONE.negate(),
                    BigInteger.ZERO,
                    BigInteger.ONE,
                    maxUnscaled);
            BlockBuilder builder = type.createBlockBuilder(null, values.size() + 1);
            for (BigInteger value : values) {
                if (type.isShort()) {
                    type.writeLong(builder, value.longValueExact());
                }
                else {
                    type.writeObject(builder, Int128.valueOf(value));
                }
            }
            builder.appendNull();

            List<Page> output = executeRoundTrip(List.of(new Page(builder.build())), List.of(type), Set.of(0));
            int totalPositions = output.stream().mapToInt(Page::getPositionCount).sum();
            assertThat(totalPositions).as("type %s", type).isEqualTo(values.size() + 1);

            Streams.forEachPair(
                    positions(output),
                    IntStream.range(0, values.size() + 1).boxed(),
                    (actualPos, expectedIndex) -> {
                        Block block = actualPos.page().getBlock(0);
                        if (expectedIndex == values.size()) {
                            assertThat(block.isNull(actualPos.position()))
                                    .as("type %s null position", type)
                                    .isTrue();
                            return;
                        }
                        BigInteger expected = values.get(expectedIndex);
                        BigInteger actual = type.isShort()
                                ? BigInteger.valueOf((Long) readNativeValue(type, block, actualPos.position()))
                                : ((Int128) readNativeValue(type, block, actualPos.position())).toBigInteger();
                        assertThat(actual)
                                .as("type %s position %d", type, expectedIndex)
                                .isEqualTo(expected);
                    });
        }
    }

    private List<Page> createInputPages(List<Integer> positionsCounts, NullsProvider nullsProvider, List<Type> types)
    {
        Block[][] pages = new Block[positionsCounts.size()][types.size()];
        for (int column = 0; column < types.size(); column++) {
            List<Block> blocks = createBlocks(positionsCounts, nullsProvider, types.get(column));
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

    /**
     * Execute round trip: CPU → GPU → CPU without any operations.
     * Pipeline: BufferPages → CopyToDevice → CopyToBlocks → Pages
     */
    private List<Page> executeRoundTrip(List<Page> inputPages, List<Type> types, Set<Integer> channelsToTransfer)
    {
        return executeGpuOperation(
                inputPages,
                types,
                types,
                copyToDevice -> copyToDevice,
                channelsToTransfer);
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
}
