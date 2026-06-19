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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Streams;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.FullConnectorSession;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Data;
import io.trino.operator.gpu.GpuOperation.Finished;
import io.trino.operator.gpu.GpuOperation.Yielded;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.Column.DeviceMemory;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.gen.TestColumnarFilters.NullsProvider;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.InternalDynamicFilter;
import io.trino.sql.planner.Symbol;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingSession;
import io.trino.type.BlockTypeOperators;
import io.trino.type.BlockTypeOperators.BlockPositionIsIdentical;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_DAY;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Math.clamp;
import static java.lang.Math.min;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public final class GpuTestUtils
{
    private GpuTestUtils() {}

    public static final List<Type> TESTED_GPU_TYPES = ImmutableList.<Type>builder()
            .add(BOOLEAN)
            .add(TINYINT)
            .add(SMALLINT)
            .add(INTEGER)
            .add(DATE)
            .add(BIGINT)
            .add(REAL)
            .add(DOUBLE)
            .add(createTimestampType(0))
            .add(createTimestampType(1))
            .add(createTimestampType(2))
            .add(createTimestampType(3))
            .add(createTimestampType(4))
            .add(createTimestampType(5))
            .add(createTimestampType(6))
            .add(createTimestampType(7))
            .add(createTimestampType(8))
            .add(createTimestampType(9))
            .add(createDecimalType(9, 2))
            .add(createDecimalType(18, 6))
            .add(createDecimalType(27, 4))
            .add(createDecimalType(38, 10))
            .add(createCharType(20))
            .add(VARCHAR)
            .add(createVarcharType(5))
            .add(createVarcharType(20))
            .add(VARBINARY)
            .build();

    private static final BlockTypeOperators BLOCK_TYPE_OPERATORS = new BlockTypeOperators();

    public static final FullConnectorSession FULL_CONNECTOR_SESSION = new FullConnectorSession(
            TestingSession.testSessionBuilder().build(),
            ConnectorIdentity.ofUser("test"));

    public static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    public static void maybeSetGpuMemoryPoolForTests()
    {
        // First call to Rmm.initialize wins.
        new GpuConfigurer(new GpuConfig()
                .setPoolSize(DataSize.of(1, DataSize.Unit.GIGABYTE))
                .setAggregationCompactionThreshold(DataSize.of(512, DataSize.Unit.MEGABYTE)),
                Optional.empty())
                .setup();
    }

    /**
     * Builds a single-column {@link GpuPage} backed by an INTEGER {@link DeviceMemory} column
     * containing the given values.
     */
    public static @Own GpuPage deviceIntColumn(int[] values)
    {
        try (DeviceMemory column = new DeviceMemory(ColumnVector.fromInts(values))) {
            return new GpuPage(values.length, new Column[] {column});
        }
    }

    public static Block createBlock(Type type, int positionsCount, NullsProvider nullsProvider)
    {
        return createBlocks(List.of(positionsCount), nullsProvider, type).getFirst();
    }

    public static Block createBigintBlock(int positionsCount, NullsProvider nullsProvider, long minValue, long maxValue)
    {
        return createBigintBlocks(List.of(positionsCount), nullsProvider, minValue, maxValue).getFirst();
    }

    public static List<Block> createBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider, Type type)
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
        if (type == DATE) {
            return createDateBlocks(positionsCounts, nullsProvider);
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
        if (type instanceof TimestampType timestampType) {
            if (timestampType.isShort()) {
                return createShortTimestampBlocks(positionsCounts, nullsProvider, timestampType);
            }
            return createLongTimestampBlocks(positionsCounts, nullsProvider, timestampType);
        }
        if (type instanceof DecimalType decimalType) {
            return decimalType.isShort()
                    ? createShortDecimalBlocks(positionsCounts, nullsProvider, decimalType)
                    : createLongDecimalBlocks(positionsCounts, nullsProvider, decimalType);
        }
        if (type instanceof CharType charType) {
            return createStringBlocks(positionsCounts, nullsProvider, Optional.of(charType.getLength()), true);
        }
        if (type instanceof VarcharType varcharType) {
            return createStringBlocks(positionsCounts, nullsProvider, varcharType.getLength(), false);
        }
        if (type == VARBINARY) {
            return createVarbinaryBlocks(positionsCounts, nullsProvider);
        }
        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private static List<Block> createBooleanBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
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

    private static List<Block> createTinyintBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
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

    private static List<Block> createSmallintBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
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

    private static List<Block> createIntegerBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
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

    private static List<Block> createDateBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Random random = new Random(42);
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = DATE.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            DATE.writeLong(builder, random.nextInt());
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private static List<Block> createBigintBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        return createBigintBlocks(positionsCounts, nullsProvider, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    private static List<Block> createBigintBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider, long minValue, long maxValue)
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
                            BIGINT.writeLong(builder, random.nextLong(minValue, maxValue));
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private static List<Block> createRealBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
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

    private static List<Block> createDoubleBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
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

    private static List<Block> createShortTimestampBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider, TimestampType type)
    {
        Random random = new Random(42);
        // Stored value is epochMicros; for precision p < 6, digits beyond p must be 0
        long scale = 1L;
        for (int i = type.getPrecision(); i < TimestampType.MAX_SHORT_PRECISION; i++) {
            scale *= 10;
        }
        long finalScale = scale;
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = type.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            type.writeLong(builder, (random.nextLong() / finalScale) * finalScale);
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private static List<Block> createLongTimestampBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider, TimestampType type)
    {
        Random random = new Random(42);
        // picosOfMicro is in [0, 10^6); for precision p, digits beyond p must be 0,
        // i.e. picosOfMicro must be a multiple of 10^(MAX_PRECISION - p).
        int picosScale = 1;
        for (int i = type.getPrecision(); i < TimestampType.MAX_PRECISION; i++) {
            picosScale *= 10;
        }
        int finalPicosScale = picosScale;
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = type.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            long epochMicros = random.nextLong();
                            // Limit to values that can be represented in 64-bit with nanosecond precision, also after e.g. date_trunc(day)
                            epochMicros = clamp(
                                    epochMicros,
                                    Long.MIN_VALUE / NANOSECONDS_PER_MICROSECOND + MICROSECONDS_PER_DAY,
                                    Long.MAX_VALUE / NANOSECONDS_PER_MICROSECOND - 1);
                            int picosOfMicro = (random.nextInt(1_000_000) / finalPicosScale) * finalPicosScale;
                            type.writeObject(builder, new LongTimestamp(epochMicros, picosOfMicro));
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private static List<Block> createLongDecimalBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider, DecimalType type)
    {
        Random random = new Random(42);
        // Bound to 10^precision-1 so the unscaled value always fits the declared precision.
        BigInteger bound = BigInteger.TEN.pow(type.getPrecision());
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = type.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            BigInteger unscaledValue;
                            do {
                                unscaledValue = new BigInteger(bound.bitLength(), random);
                            }
                            while (unscaledValue.compareTo(bound) >= 0);
                            if (random.nextBoolean()) {
                                unscaledValue = unscaledValue.negate();
                            }
                            type.writeObject(builder, Int128.valueOf(unscaledValue));
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private static List<Block> createShortDecimalBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider, DecimalType type)
    {
        Random random = new Random(42);
        long bound = 1L;
        for (int i = 0; i < type.getPrecision(); i++) {
            bound *= 10;
        }
        long finalBound = bound;
        return positionsCounts.stream()
                .map(positionsCount -> {
                    Optional<boolean[]> isNull = nullsProvider.getNulls(positionsCount);
                    assertThat(isNull.isEmpty() || isNull.get().length == positionsCount).isTrue();
                    BlockBuilder builder = type.createBlockBuilder(null, positionsCount);
                    for (int i = 0; i < positionsCount; i++) {
                        if (isNull.isPresent() && isNull.get()[i]) {
                            builder.appendNull();
                        }
                        else {
                            type.writeLong(builder, random.nextLong(-(finalBound - 1), finalBound));
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private static List<Block> createStringBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider, Optional<Integer> lengthLimit, boolean trimTrailingSpaces)
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
                            String next = strings.next();
                            if (lengthLimit.isPresent()) {
                                next = next.substring(0, min(next.length(), lengthLimit.get()));
                            }
                            if (trimTrailingSpaces) {
                                next = next.stripTrailing();
                            }
                            builder.writeEntry(Slices.utf8Slice(next));
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    private static List<Block> createVarbinaryBlocks(List<Integer> positionsCounts, NullsProvider nullsProvider)
    {
        Iterator<byte[]> bytes = generateInputBytes().iterator();
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
                            builder.writeEntry(Slices.wrappedBuffer(bytes.next()));
                        }
                    }
                    return builder.build();
                })
                .collect(toImmutableList());
    }

    /**
     * Cycle through a fixture of byte arrays mixed with random payloads. Includes byte
     * sequences that are not valid UTF-8 to verify the GPU path is byte-transparent.
     */
    private static Stream<byte[]> generateInputBytes()
    {
        List<byte[]> fixtures = ImmutableList.<byte[]>builder()
                .add(new byte[0])
                .add(new byte[] {0x00})
                .add(new byte[] {0x00, 0x01, 0x02, 0x03})
                .add(new byte[] {(byte) 0xFF, (byte) 0xFE, (byte) 0xFD, (byte) 0xFC})
                .add(new byte[] {(byte) 0xC0, (byte) 0x80}) // overlong NUL — not valid UTF-8
                .add(new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80}) // unpaired surrogate — not valid UTF-8
                .add("test".getBytes(UTF_8))
                .add("the quick brown fox".getBytes(UTF_8))
                .add(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF})
                .add(new byte[256])
                .build();
        Random random = new Random(42);
        Stream<byte[]> randomBytes = Stream.generate(() -> {
            byte[] b = new byte[random.nextInt(64)];
            random.nextBytes(b);
            return b;
        });
        return Streams.zip(
                        Stream.generate(() -> fixtures).flatMap(List::stream),
                        randomBytes,
                        List::of)
                .flatMap(List::stream);
    }

    /**
     * Generate infinite stream of test strings by cycling through testStrings
     * and mixing with random strings for variety.
     */
    private static Stream<String> generateInputStrings()
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

    public static List<Page> executeWithCpu(
            List<Page> inputPages,
            List<Expression> expressions,
            Map<Symbol, Integer> layout)
    {
        PageProcessor pageProcessor = compileCpuExpressions(expressions, layout);
        return executeWithCpu(pageProcessor, inputPages);
    }

    public static PageProcessor compileCpuExpression(Expression expression, Map<Symbol, Integer> layout)
    {
        return compileCpuExpressions(List.of(expression), layout);
    }

    public static PageProcessor compileCpuExpressions(List<Expression> expressions, Map<Symbol, Integer> layout)
    {
        return FUNCTION_RESOLUTION.getExpressionCompiler().compilePageProcessor(
                        false,
                        true,
                        false,
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        expressions,
                        layout,
                        Optional.empty(),
                        OptionalInt.empty())
                .apply(InternalDynamicFilter.EMPTY);
    }

    public static List<Page> executeWithCpu(PageProcessor compiledProcessor, List<Page> inputPages)
    {
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

    public static @Move List<GpuPage> copyToDevice(List<Page> pages, List<Type> types)
    {
        Set<Integer> deviceChannels = IntStream.range(0, types.size()).boxed().collect(toImmutableSet());
        Iterator<Page> input = pages.iterator();
        @Own List<GpuPage> result = new ArrayList<>();
        GpuOperation.Context context = new TestingGpuOperationContext();
        try (BufferPages bufferPages = new BufferPages();
                CopyToDevice copyToDevice = new CopyToDevice(context, bufferPages, types, deviceChannels)) {
            while (true) {
                switch (copyToDevice.execute()) {
                    case Yielded() -> {
                        if (input.hasNext()) {
                            bufferPages.addInput(input.next());
                        }
                        else {
                            bufferPages.noMoreInput();
                        }
                    }

                    case Blocked _ -> throw new UnsupportedOperationException("Unsupported blocked future, what shall I do?");
                    case Data(var page) -> result.add(page);
                    case Finished() -> {
                        return result;
                    }
                }
            }
        }
        catch (Throwable e) {
            closeAllSuppress(e, result.toArray(GpuPage[]::new));
            throw e;
        }
    }

    public static List<Page> executeGpuOperation(
            List<Page> inputPages,
            List<Type> inputTypes,
            List<Type> outputTypes,
            BiFunction<GpuOperation.Context, CopyToDevice, GpuOperation> operationFactory)
    {
        Set<Integer> deviceChannels = IntStream.range(0, inputTypes.size())
                .boxed()
                .collect(toImmutableSet());
        return executeGpuOperation(inputPages, inputTypes, outputTypes, operationFactory, deviceChannels);
    }

    public static List<Page> executeGpuOperation(
            List<Page> inputPages,
            List<Type> inputTypes,
            List<Type> outputTypes,
            BiFunction<GpuOperation.Context, CopyToDevice, GpuOperation> operationFactory,
            Set<Integer> deviceChannels)
    {
        Iterator<Page> input = inputPages.iterator();
        GpuOperation.Context context = new TestingGpuOperationContext();
        try (BufferPages bufferPages = new BufferPages();
                CopyToDevice copyToDevice = new CopyToDevice(context, bufferPages, inputTypes, deviceChannels);
                GpuOperation operation = operationFactory.apply(context, copyToDevice);
                CopyToBlocks copyToBlocks = new CopyToBlocks(operation, outputTypes)) {
            return drainToPages(
                    () -> {
                        if (!input.hasNext()) {
                            bufferPages.noMoreInput();
                        }
                        else if (bufferPages.needsInput()) {
                            bufferPages.addInput(input.next());
                        }
                    },
                    copyToBlocks);
        }
    }

    public static List<Page> drainToPages(Runnable feedInput, GpuOperation outout)
    {
        GpuPageToPages gpuPageToPages = new GpuPageToPages();

        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();
        while (true) {
            feedInput.run();

            gpuPageToPages.drain().forEachOrdered(outputPages::add);

            @Own GpuOperation.Result result = outout.execute();
            switch (result) {
                case Blocked _ -> throw new UnsupportedOperationException("Unsupported blocked future, what shall I do?");
                case Data(GpuPage gpuPage) -> {
                    try (gpuPage) {
                        gpuPageToPages.add(gpuPage);
                    }
                }
                case Yielded() -> {
                    // continue
                }
                case Finished() -> {
                    checkState(gpuPageToPages.poll().isEmpty(), "gpuPageToPages should be drained at this point");
                    return outputPages.build();
                }
            }
        }
    }

    public static void assertSameDataWithoutOrder(List<Page> actual, List<Page> expected, List<Type> types)
    {
        MaterializedResult actualResult = MaterializedResult.resultBuilder(FULL_CONNECTOR_SESSION, types)
                .pages(actual)
                .build();
        MaterializedResult expectedResult = MaterializedResult.resultBuilder(FULL_CONNECTOR_SESSION, types)
                .pages(expected)
                .build();
        assertThat(ImmutableMultiset.copyOf(actualResult.getMaterializedRows()))
                .isEqualTo(ImmutableMultiset.copyOf(expectedResult.getMaterializedRows()));
    }

    public static void assertSameDataInOrder(List<Page> actual, List<Page> expected, List<Type> types)
    {
        assertThat(actual.stream().mapToInt(Page::getPositionCount).sum()).as("actual position count (sum over all returned pages)")
                .isEqualTo(expected.stream().mapToInt(Page::getPositionCount).sum());
        Stream.concat(actual.stream(), expected.stream())
                .forEach(page -> assertThat(page.getChannelCount()).as("channel count").isEqualTo(types.size()));

        List<BlockPositionIsIdentical> identicalOperators = types.stream()
                .map(BLOCK_TYPE_OPERATORS::getIdenticalOperator)
                .toList();

        Streams.forEachPair(
                positions(actual),
                positions(expected),
                (actualPos, expectedPos) -> {
                    for (int channel = 0; channel < types.size(); channel++) {
                        BlockPositionIsIdentical equivalence = identicalOperators.get(channel);
                        Block actualBlock = actualPos.page.getBlock(channel);
                        Block expectedBlock = expectedPos.page.getBlock(channel);
                        if (!equivalence.isIdentical(actualBlock, actualPos.position, expectedBlock, expectedPos.position)) {
                            throw new AssertionError("row %d channel %d (type %s): actual=«%s» expected=«%s»".formatted(
                                    actualPos.position,
                                    channel,
                                    types.get(channel),
                                    types.get(channel).getObjectValue(actualBlock, actualPos.position),
                                    types.get(channel).getObjectValue(expectedBlock, expectedPos.position)));
                        }
                    }
                });
    }

    public static Stream<PagePosition> positions(List<Page> pages)
    {
        return pages.stream().flatMap(GpuTestUtils::positions);
    }

    private static Stream<PagePosition> positions(Page page)
    {
        return IntStream.range(0, page.getPositionCount())
                .mapToObj(i -> new PagePosition(page, i));
    }

    public record PagePosition(Page page, int position) {}
}
