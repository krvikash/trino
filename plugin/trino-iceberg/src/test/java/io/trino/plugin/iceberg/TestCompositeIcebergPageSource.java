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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

final class TestCompositeIcebergPageSource
{
    @Test
    void testHappyPath()
            throws IOException
    {
        List<IcebergSplit> splits = createSplits(3);
        List<SourcePage> pages1 = List.of(SourcePage.create(10), SourcePage.create(20));
        List<SourcePage> pages2 = List.of(SourcePage.create(30));
        List<SourcePage> pages3 = List.of(SourcePage.create(5), SourcePage.create(15));

        TestingPageSource source1 = new TestingPageSource(pages1, 100, 1000);
        TestingPageSource source2 = new TestingPageSource(pages2, 200, 2000);
        TestingPageSource source3 = new TestingPageSource(pages3, 300, 3000);
        List<TestingPageSource> sources = List.of(source1, source2, source3);

        try (CompositeIcebergPageSource composite = new CompositeIcebergPageSource(splits, split -> sources.get(splits.indexOf(split)))) {
            List<SourcePage> result = drainPages(composite);
            assertThat(result).hasSize(5);
            assertThat(result.get(0).getPositionCount()).isEqualTo(10);
            assertThat(result.get(1).getPositionCount()).isEqualTo(20);
            assertThat(result.get(2).getPositionCount()).isEqualTo(30);
            assertThat(result.get(3).getPositionCount()).isEqualTo(5);
            assertThat(result.get(4).getPositionCount()).isEqualTo(15);
            assertThat(composite.isFinished()).isTrue();
            assertThat(composite.getCompletedBytes()).isEqualTo(600);
            assertThat(composite.getReadTimeNanos()).isEqualTo(6000);
        }

        assertThat(source1.closed).isTrue();
        assertThat(source2.closed).isTrue();
        assertThat(source3.closed).isTrue();
    }

    @Test
    void testEmptySubSource()
            throws IOException
    {
        List<IcebergSplit> splits = createSplits(3);
        TestingPageSource source1 = new TestingPageSource(List.of(SourcePage.create(10)), 100, 1000);
        TestingPageSource emptySource = new TestingPageSource(List.of(), 0, 0);
        TestingPageSource source3 = new TestingPageSource(List.of(SourcePage.create(20)), 200, 2000);
        List<TestingPageSource> sources = List.of(source1, emptySource, source3);

        try (CompositeIcebergPageSource composite = new CompositeIcebergPageSource(splits, split -> sources.get(splits.indexOf(split)))) {
            List<SourcePage> result = drainPages(composite);
            assertThat(result).hasSize(2);
            assertThat(result.get(0).getPositionCount()).isEqualTo(10);
            assertThat(result.get(1).getPositionCount()).isEqualTo(20);
            assertThat(composite.getCompletedBytes()).isEqualTo(300);
            assertThat(composite.getReadTimeNanos()).isEqualTo(3000);
        }

        assertThat(emptySource.closed).isTrue();
    }

    @Test
    void testMetricsMerging()
            throws IOException
    {
        List<IcebergSplit> splits = createSplits(2);
        Metrics metrics1 = new Metrics(ImmutableMap.of("rows_read", new LongCount(5)));
        Metrics metrics2 = new Metrics(ImmutableMap.of("rows_read", new LongCount(7)));
        TestingPageSource source1 = new TestingPageSource(List.of(SourcePage.create(10)), 0, 0, metrics1);
        TestingPageSource source2 = new TestingPageSource(List.of(SourcePage.create(10)), 0, 0, metrics2);
        List<TestingPageSource> sources = List.of(source1, source2);

        try (CompositeIcebergPageSource composite = new CompositeIcebergPageSource(splits, split -> sources.get(splits.indexOf(split)))) {
            drainPages(composite);
            Metrics merged = composite.getMetrics();
            assertThat(((LongCount) merged.getMetrics().get("rows_read")).getTotal()).isEqualTo(12);
        }
    }

    @Test
    void testEarlyClose()
            throws IOException
    {
        List<IcebergSplit> splits = createSplits(3);
        TestingPageSource source1 = new TestingPageSource(List.of(SourcePage.create(10)), 100, 0);
        TestingPageSource source2 = new TestingPageSource(List.of(SourcePage.create(20), SourcePage.create(30)), 200, 0);
        AtomicBoolean thirdFactoryCalled = new AtomicBoolean(false);

        CompositeIcebergPageSource composite = new CompositeIcebergPageSource(splits, split -> {
            int index = splits.indexOf(split);
            if (index == 0) {
                return source1;
            }
            if (index == 1) {
                return source2;
            }
            thirdFactoryCalled.set(true);
            return new TestingPageSource(List.of(), 0, 0);
        });

        // Read through the first source and one page from the second
        assertThat(composite.getNextSourcePage()).isNotNull();
        assertThat(composite.getNextSourcePage()).isNotNull();
        assertThat(source1.closed).isTrue();

        composite.close();

        assertThat(composite.isFinished()).isTrue();
        assertThat(source2.closed).isTrue();
        assertThat(thirdFactoryCalled.get()).isFalse();
    }

    @Test
    void testMemoryUsage()
    {
        List<IcebergSplit> splits = createSplits(2);
        TestingPageSource source1 = new TestingPageSource(List.of(SourcePage.create(10)), 0, 0, Metrics.EMPTY, 1024);
        TestingPageSource source2 = new TestingPageSource(List.of(SourcePage.create(10)), 0, 0, Metrics.EMPTY, 2048);
        List<TestingPageSource> sources = List.of(source1, source2);

        CompositeIcebergPageSource composite = new CompositeIcebergPageSource(splits, split -> sources.get(splits.indexOf(split)));

        assertThat(composite.getMemoryUsage()).isEqualTo(0);

        composite.getNextSourcePage();
        assertThat(composite.getMemoryUsage()).isEqualTo(1024);

        // Exhaust first source, advance to second
        assertThat(composite.getNextSourcePage()).isNotNull();
        assertThat(composite.getMemoryUsage()).isEqualTo(2048);

        drainPages(composite);
        assertThat(composite.getMemoryUsage()).isEqualTo(0);
    }

    private static List<IcebergSplit> createSplits(int count)
    {
        ImmutableList.Builder<IcebergSplit> builder = ImmutableList.builder();
        for (int i = 0; i < count; i++) {
            builder.add(new IcebergSplit(
                    "path_" + i,
                    0,
                    100,
                    100,
                    25,
                    IcebergFileFormat.PARQUET,
                    0,
                    0,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    SplitWeight.standard(),
                    TupleDomain.all(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    Optional.empty()));
        }
        return builder.build();
    }

    private static List<SourcePage> drainPages(CompositeIcebergPageSource composite)
    {
        List<SourcePage> pages = new ArrayList<>();
        while (!composite.isFinished()) {
            SourcePage page = composite.getNextSourcePage();
            if (page != null) {
                pages.add(page);
            }
        }
        return pages;
    }

    private static class TestingPageSource
            implements ConnectorPageSource
    {
        private final List<SourcePage> pages;
        private final long completedBytes;
        private final long readTimeNanos;
        private final Metrics metrics;
        private final long memoryUsage;
        private int index;
        boolean closed;

        TestingPageSource(List<SourcePage> pages, long completedBytes, long readTimeNanos)
        {
            this(pages, completedBytes, readTimeNanos, Metrics.EMPTY, 0);
        }

        TestingPageSource(List<SourcePage> pages, long completedBytes, long readTimeNanos, Metrics metrics)
        {
            this(pages, completedBytes, readTimeNanos, metrics, 0);
        }

        TestingPageSource(List<SourcePage> pages, long completedBytes, long readTimeNanos, Metrics metrics, long memoryUsage)
        {
            this.pages = List.copyOf(pages);
            this.completedBytes = completedBytes;
            this.readTimeNanos = readTimeNanos;
            this.metrics = metrics;
            this.memoryUsage = memoryUsage;
        }

        @Override
        public long getCompletedBytes()
        {
            return completedBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return readTimeNanos;
        }

        @Override
        public boolean isFinished()
        {
            return index >= pages.size();
        }

        @Override
        public SourcePage getNextSourcePage()
        {
            if (index >= pages.size()) {
                return null;
            }
            return pages.get(index++);
        }

        @Override
        public long getMemoryUsage()
        {
            return memoryUsage;
        }

        @Override
        public Metrics getMetrics()
        {
            return metrics;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
