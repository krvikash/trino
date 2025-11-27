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
package io.trino;

import com.google.common.collect.ImmutableList;
import io.trino.operator.PagesIndex;
import io.trino.operator.PagesIndexPageSorter;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterators.cycle;
import static com.google.common.collect.Iterators.limit;
import static io.trino.block.BlockAssertions.createRandomBlockForType;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Collections.nCopies;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkPagesIndexPageSorter
{
    @Benchmark
    public int runBenchmark(BenchmarkData data)
    {
        PageSorter pageSorter = new PagesIndexPageSorter(new PagesIndex.TestingFactory(false));
        Iterator<Page> outputPages = pageSorter.sort(data.types, data.pages, data.sortChannels, nCopies(data.sortChannels.size(), ASC_NULLS_FIRST), 10_000);
        int totalPositions = 0;
        while (outputPages.hasNext()) {
            Page page = outputPages.next();
            totalPositions += page.getPositionCount();
        }
        return totalPositions;
    }

    private static List<Page> createPages(int pageCount, int channelCount, List<Type> types)
    {
        List<Page> pages = new ArrayList<>(pageCount);
        for (int numPage = 0; numPage < pageCount; numPage++) {
            Block[] blocks = new Block[channelCount];
            for (int numChannel = 0; numChannel < channelCount; numChannel++) {
                Type type = types.get(numChannel);
                blocks[numChannel] = createRandomBlockForType(type, 4096, 0.1f);
            }
            pages.add(new Page(4096, blocks));
        }
        return pages;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"1", "2", "3", "4"})
        private int numSortChannels;

        private List<Page> pages;
        private final int maxPages = 500;

        public List<Type> types;
        public List<Integer> sortChannels;

        @Setup
        public void setup()
        {
            int totalChannels = 20;

            types = ImmutableList.copyOf(limit(cycle(ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BOOLEAN)), totalChannels));
            pages = createPages(maxPages, totalChannels, types);

            sortChannels = new ArrayList<>();
            for (int i = 0; i < numSortChannels; i++) {
                sortChannels.add(i);
            }
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkPagesIndexPageSorter.class).run();
    }
}
