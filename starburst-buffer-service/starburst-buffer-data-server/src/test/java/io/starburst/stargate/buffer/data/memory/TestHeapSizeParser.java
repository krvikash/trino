/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.stargate.buffer.data.memory;

import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;
import java.util.stream.Stream;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHeapSizeParser
{
    private static final Supplier<Long> MAX_HEAP_MEMORY = () -> DataSize.of(8, GIGABYTE).toBytes();
    private static final HeapSizeParser HEAP_SIZE_PARSER = new HeapSizeParser(MAX_HEAP_MEMORY);

    @Test
    void testAbsoluteMemory()
    {
        assertThat(HEAP_SIZE_PARSER.parse("1GB")).isEqualTo(DataSize.of(1, GIGABYTE));
        assertThat(HEAP_SIZE_PARSER.parse("8GB")).isEqualTo(DataSize.of(8, GIGABYTE));

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("9GB"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be greater than maximum heap size");

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("0B"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be less than or equal to 0");
    }

    @Test
    void testRelativeMemory()
    {
        assertThat(HEAP_SIZE_PARSER.parse("12.5%")).isEqualTo(DataSize.of(1, GIGABYTE));
        assertThat(HEAP_SIZE_PARSER.parse("100%")).isEqualTo(DataSize.of(8, GIGABYTE));
        assertThat(HEAP_SIZE_PARSER.parse("50%")).isEqualTo(DataSize.of(4, GIGABYTE));

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("125%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be greater than maximum heap size");

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("0%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be less than or equal to 0");
    }

    @Test
    void testRelativeMemoryRoundsDown()
    {
        HeapSizeParser parser = new HeapSizeParser(() -> 10L);
        // 3.5 and 3.9 would round to 4 with round-to-nearest
        assertThat(parser.parse("35%").toBytes()).isEqualTo(3);
        assertThat(parser.parse("39%").toBytes()).isEqualTo(3);
    }

    @Test
    void testRelativeSlicesNeverExceedHeap()
    {
        // Each split exceeds the heap when rounding to nearest
        assertSlicesFitInHeap(52982448128L, "60%", "20%", "20%");
        assertSlicesFitInHeap(7, "50%", "50%");
        assertSlicesFitInHeap(5, "30%", "30%", "40%");
        assertSlicesFitInHeap(6, "25%", "25%", "25%", "25%");
    }

    @Test
    void testRelativeMemoryIsExact()
    {
        // In double arithmetic 29 / 100.0 * 100 is 28.999999999999996, which would floor to 28
        assertThat(new HeapSizeParser(() -> 100L).parse("29%").toBytes()).isEqualTo(29);

        // 2^53 + 1 is not representable as a double
        long maxHeapMemory = (1L << 53) + 1;
        assertThat(new HeapSizeParser(() -> maxHeapMemory).parse("100%").toBytes()).isEqualTo(maxHeapMemory);

        assertThat(new HeapSizeParser(() -> 52982448128L).parse("19.99%").toBytes()).isEqualTo(10591191380L);
    }

    @Test
    void testRelativeMemoryEdgeCases()
    {
        assertThat(HEAP_SIZE_PARSER.parse(" 50 %")).isEqualTo(DataSize.of(4, GIGABYTE));

        // Rounds down to 0 bytes
        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("0.000000001%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be less than or equal to 0");

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("-5%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be less than or equal to 0");

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("1e30%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be greater than maximum heap size");

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("-1e30%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be less than or equal to 0");

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("%"))
                .isInstanceOf(NumberFormatException.class);
    }

    @Test
    void testNonPositiveMaxHeap()
    {
        assertThatThrownBy(() -> new HeapSizeParser(() -> 0L).parse("50%"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("maxHeapMemory must be positive");
    }

    private static void assertSlicesFitInHeap(long maxHeapMemory, String... values)
    {
        HeapSizeParser parser = new HeapSizeParser(() -> maxHeapMemory);
        long total = Stream.of(values)
                .mapToLong(value -> parser.parse(value).toBytes())
                .sum();
        assertThat(total).isLessThanOrEqualTo(maxHeapMemory);
    }
}
