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
package io.trino.spi;

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.PreSizedPageBuilder.DEFAULT_INITIAL_CAPACITY;
import static io.trino.spi.PreSizedPageBuilder.MAX_ENTRIES;
import static io.trino.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestPreSizedPageBuilder
{
    @Test
    void testWithNoColumns()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of());
        assertThat(pageBuilder.isEmpty()).isTrue();
        assertThat(pageBuilder.getPositionCount()).isEqualTo(0);

        pageBuilder.declarePosition();
        pageBuilder.declarePosition();
        Page page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(page.getChannelCount()).isEqualTo(0);
        assertThat(page.getSizeInBytes()).isEqualTo(0);

        pageBuilder.reset();
        assertThat(pageBuilder.isEmpty()).isTrue();
        page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(0);
    }

    @Test
    void testDeclarePosition()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT, VARCHAR));
        assertThat(pageBuilder.isEmpty()).isTrue();
        assertThat(pageBuilder.getPositionCount()).isEqualTo(0);

        pageBuilder.declarePosition();
        assertThat(pageBuilder.isEmpty()).isFalse();
        assertThat(pageBuilder.getPositionCount()).isEqualTo(1);

        pageBuilder.declarePosition();
        assertThat(pageBuilder.getPositionCount()).isEqualTo(2);

        pageBuilder.declarePosition();
        assertThat(pageBuilder.getPositionCount()).isEqualTo(3);
    }

    @Test
    void testIsFull()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT));

        for (int i = 0; i < DEFAULT_INITIAL_CAPACITY; i++) {
            assertThat(pageBuilder.isFull()).isFalse();
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), i);
        }

        // After declaring DEFAULT_INITIAL_EXPECTED_ENTRIES positions, it should be full
        assertThat(pageBuilder.isFull()).isTrue();
        assertThat(pageBuilder.build().getPositionCount()).isEqualTo(DEFAULT_INITIAL_CAPACITY);
        pageBuilder.reset();
        assertThat(pageBuilder.isFull()).isFalse();
        assertThat(pageBuilder.isEmpty()).isTrue();
    }

    @Test
    void testIsEmpty()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT, VARCHAR));
        assertThat(pageBuilder.isEmpty()).isTrue();

        pageBuilder.declarePosition();
        assertThat(pageBuilder.isEmpty()).isFalse();

        pageBuilder.declarePosition();
        assertThat(pageBuilder.isEmpty()).isFalse();
    }

    @Test
    void testBuildWithMultipleTypes()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT, VARCHAR, DOUBLE, BOOLEAN));

        // Add first row
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 100L);
        VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("hello"));
        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(2), 1.5);
        BOOLEAN.writeBoolean(pageBuilder.getBlockBuilder(3), true);
        pageBuilder.declarePosition();

        // Add second row
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 200L);
        VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("world"));
        DOUBLE.writeDouble(pageBuilder.getBlockBuilder(2), 2.5);
        BOOLEAN.writeBoolean(pageBuilder.getBlockBuilder(3), false);
        pageBuilder.declarePosition();

        Page page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(page.getChannelCount()).isEqualTo(4);

        assertThat(BIGINT.getLong(page.getBlock(0), 0)).isEqualTo(100L);
        assertThat(VARCHAR.getSlice(page.getBlock(1), 0).toStringUtf8()).isEqualTo("hello");
        assertThat(DOUBLE.getDouble(page.getBlock(2), 0)).isEqualTo(1.5);
        assertThat(BOOLEAN.getBoolean(page.getBlock(3), 0)).isTrue();

        assertThat(BIGINT.getLong(page.getBlock(0), 1)).isEqualTo(200L);
        assertThat(VARCHAR.getSlice(page.getBlock(1), 1).toStringUtf8()).isEqualTo("world");
        assertThat(DOUBLE.getDouble(page.getBlock(2), 1)).isEqualTo(2.5);
        assertThat(BOOLEAN.getBoolean(page.getBlock(3), 1)).isFalse();
    }

    @Test
    void testBuildWithNullValues()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT, VARCHAR));

        // Add first row with null values
        pageBuilder.getBlockBuilder(0).appendNull();
        pageBuilder.getBlockBuilder(1).appendNull();
        pageBuilder.declarePosition();

        // Add second row with values
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 100L);
        VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("test"));
        pageBuilder.declarePosition();

        // Add third row with mixed nulls
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 200L);
        pageBuilder.getBlockBuilder(1).appendNull();
        pageBuilder.declarePosition();

        Page page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(3);

        assertThat(page.getBlock(0).isNull(0)).isTrue();
        assertThat(page.getBlock(1).isNull(0)).isTrue();

        assertThat(page.getBlock(0).isNull(1)).isFalse();
        assertThat(BIGINT.getLong(page.getBlock(0), 1)).isEqualTo(100L);
        assertThat(VARCHAR.getSlice(page.getBlock(1), 1).toStringUtf8()).isEqualTo("test");

        assertThat(BIGINT.getLong(page.getBlock(0), 2)).isEqualTo(200L);
        assertThat(page.getBlock(1).isNull(2)).isTrue();
    }

    @Test
    void testBuildWithEmptyPage()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT));

        Page page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(0);
        assertThat(page.getChannelCount()).isEqualTo(1);

        pageBuilder.reset();

        assertThat(pageBuilder.isEmpty()).isTrue();
        assertThat(pageBuilder.getPositionCount()).isEqualTo(0);

        while (!pageBuilder.isFull()) {
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 123);
            pageBuilder.declarePosition();
        }
        assertThat(pageBuilder.build().getPositionCount()).isEqualTo(DEFAULT_INITIAL_CAPACITY);
    }

    @Test
    void testBuildThrowsWhenBlockPositionsMismatch()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT, VARCHAR));

        // Add value to first block but not second
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 100L);
        pageBuilder.declarePosition();

        // Add value only to first block (second block is missing a value)
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 200L);
        pageBuilder.declarePosition();

        // This should throw because blocks have mismatched position counts
        assertThatThrownBy(pageBuilder::build)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Declared positions")
                .hasMessageContaining("does not match");
    }

    @Test
    void testReset()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT, VARCHAR));

        // Add some values and build a page
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 100L);
        VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("test"));
        pageBuilder.declarePosition();

        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 200L);
        VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("hello"));
        pageBuilder.declarePosition();

        Page firstPage = pageBuilder.build();
        assertThat(firstPage.getPositionCount()).isEqualTo(2);

        // Reset the page builder
        pageBuilder.reset();

        // Verify it's empty after reset
        assertThat(pageBuilder.isEmpty()).isTrue();
        assertThat(pageBuilder.getPositionCount()).isEqualTo(0);
        assertThat(pageBuilder.isFull()).isFalse();

        // Add new values
        BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 300L);
        VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("world"));
        pageBuilder.declarePosition();

        Page secondPage = pageBuilder.build();
        assertThat(secondPage.getPositionCount()).isEqualTo(1);
        assertThat(BIGINT.getLong(secondPage.getBlock(0), 0)).isEqualTo(300L);
        assertThat(VARCHAR.getSlice(secondPage.getBlock(1), 0).toStringUtf8()).isEqualTo("world");

        // Verify first page is unchanged
        assertThat(firstPage.getPositionCount()).isEqualTo(2);
    }

    @Test
    void testResetReduceCapacityAfterLargeEntries()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(VARCHAR));

        // Fill the page builder with large entries
        while (!pageBuilder.isFull()) {
            pageBuilder.declarePosition();
            byte[] bytes = new byte[500 * 1024]; // 500 KB entries
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(0), Slices.wrappedBuffer(bytes));
        }

        Page page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(DEFAULT_INITIAL_CAPACITY);

        // After reset, the expected entries should be adjusted based on the previous page size
        pageBuilder.reset();

        assertThat(pageBuilder.isEmpty()).isTrue();
        assertThat(pageBuilder.getPositionCount()).isEqualTo(0);

        while (!pageBuilder.isFull()) {
            pageBuilder.declarePosition();
            byte[] bytes = new byte[500 * 1024]; // 500 KB entries
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(0), Slices.wrappedBuffer(bytes));
        }

        page = pageBuilder.build();
        assertThat(page.getPositionCount()).isEqualTo(2);
        assertThat(page.getSizeInBytes()).isLessThanOrEqualTo(DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
    }

    @Test
    void testMultipleResetAndBuildCycles()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT, VARCHAR));

        int lastPagePositionsCount = 0;
        for (int cycle = 0; cycle < 40; cycle++) {
            while (!pageBuilder.isFull()) {
                BIGINT.writeLong(pageBuilder.getBlockBuilder(0), cycle);
                VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("cycle" + cycle));
                pageBuilder.declarePosition();
            }

            Page page = pageBuilder.build();
            assertThat(page.getPositionCount()).isLessThanOrEqualTo(MAX_ENTRIES);
            if (lastPagePositionsCount >= MAX_ENTRIES / 2) {
                assertThat(page.getPositionCount()).isGreaterThanOrEqualTo(lastPagePositionsCount);
            }
            else {
                assertThat(page.getPositionCount()).isGreaterThan((int) (1.5 * lastPagePositionsCount));
            }
            assertThat(page.getSizeInBytes()).isLessThanOrEqualTo(DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
            lastPagePositionsCount = page.getPositionCount();

            pageBuilder.reset();
            assertThat(pageBuilder.isEmpty()).isTrue();
        }
    }

    @Test
    void testPageCapacityGrowth()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT, VARCHAR));

        // First page
        for (int i = 0; i < 5; i++) {
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), i);
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("test" + i));
            pageBuilder.declarePosition();
        }

        Page firstPage = pageBuilder.build();
        assertThat(firstPage.getPositionCount()).isEqualTo(5);
        long firstPageSize = firstPage.getSizeInBytes();
        assertThat(firstPageSize).isGreaterThan(0);

        // Reset and build another page
        pageBuilder.reset();

        for (int i = 0; i < 5; i++) {
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), i + 100);
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("value" + i));
            pageBuilder.declarePosition();
        }

        Page secondPage = pageBuilder.build();
        assertThat(secondPage.getPositionCount()).isEqualTo(5);

        // The page builder should track the previous size and allow 10 entries in the next build
        pageBuilder.reset();
        while (!pageBuilder.isFull()) {
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), 123);
            VARCHAR.writeSlice(pageBuilder.getBlockBuilder(1), Slices.utf8Slice("value"));
            pageBuilder.declarePosition();
        }
        assertThat(pageBuilder.build().getPositionCount()).isEqualTo(10);
    }

    @Test
    void testGetPositionCount()
    {
        PreSizedPageBuilder pageBuilder = new PreSizedPageBuilder(List.of(BIGINT));
        assertThat(pageBuilder.getPositionCount()).isEqualTo(0);

        int expectedPositions = 0;
        for (int i = 1; i <= 500; i++) {
            pageBuilder.declarePosition();
            BIGINT.writeLong(pageBuilder.getBlockBuilder(0), i);
            expectedPositions++;
            assertThat(pageBuilder.getPositionCount()).isEqualTo(expectedPositions);
            if (pageBuilder.isFull()) {
                assertThat(pageBuilder.build().getPositionCount()).isEqualTo(expectedPositions);
                pageBuilder.reset();
                expectedPositions = 0;
            }
        }
    }
}
