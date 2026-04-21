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
package io.trino.tests.benchmark;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Scalar;
import io.airlift.units.DataSize;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystem;
import io.trino.operator.gpu.BufferPages;
import io.trino.operator.gpu.CopyToDevice;
import io.trino.operator.gpu.GpuConfig;
import io.trino.operator.gpu.GpuConfigurer;
import io.trino.operator.gpu.GpuOperation;
import io.trino.operator.gpu.GpuOperation.Blocked;
import io.trino.operator.gpu.GpuOperation.Result;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.gpu.Column;
import io.trino.spi.gpu.GpuPage;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.predicate.TupleDomain;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Read `SearchPhrase` column from ClickBench Parquet files on the CPU using Trino Parquet reader and then copying data into GPU memory.
 */
public class ClickBenchQ06TableScanCpuPlusCopy
{
    private ClickBenchQ06TableScanCpuPlusCopy() {}

    static final int WARMUPS = 10;
    static final int MEASURED = 20;
    static final int SPLIT_SIZE = 64 * 1024 * 1024; // new HiveConfig().getMaxSplitSize()
//    static final int SPLIT_SIZE = 512 * 1024 * 1024; // 2 splits per file

    static void main()
            throws Exception
    {
        System.out.println("WARMUPS = " + WARMUPS);
        System.out.println("MEASURED = " + MEASURED);
        System.out.println("SPLIT_SIZE = " + SPLIT_SIZE + " " + DataSize.ofBytes(SPLIT_SIZE).succinct());

        new GpuConfigurer(new GpuConfig()); // apply defaults

        for (int i = 0; i < WARMUPS; i++) {
            run();
        }

        long elapsedMillis = 0;
        for (int i = 0; i < MEASURED; i++) {
            elapsedMillis += run();
        }
        System.out.println("Measured average: %s ms".formatted(elapsedMillis / MEASURED));
    }

    private static long run()
            throws IOException, InterruptedException, ExecutionException
    {
        long startNanoTime = nanoTime();
        int cores = Runtime.getRuntime().availableProcessors();
        int pendingSplits = 0;
        try (ExecutorService executor = Executors.newFixedThreadPool(cores * 2)) {
            CompletionService<Long> completionService = new ExecutorCompletionService<>(executor);
            Path tablePath = BenchmarkClickBench.dataLocation();
            LocalFileSystem fileSystem = new LocalFileSystem(tablePath);
            FileIterator files = fileSystem.listFiles(Location.of("local:///"));
            while (files.hasNext()) {
                FileEntry next = files.next();
                long offset = 0;
                while (offset < next.length()) {
                    completionService.submit(new Split(
                            fileSystem,
                            next.location(),
                            next.length(),
                            offset,
                            SPLIT_SIZE));
                    pendingSplits++;
                    offset += SPLIT_SIZE;
                }
            }

            long sum = 0;
            while (pendingSplits > 0) {
                sum += completionService.take().get();
                pendingSplits--;
            }
            long elapsedMillis = NANOSECONDS.toMillis(nanoTime() - startNanoTime);
            System.out.println("All splits completed in %s ms (proof of work %s)".formatted(elapsedMillis, sum));
            return elapsedMillis;
        }
    }

    private static class Split
            implements Callable<Long>
    {
        private final TrinoFileSystem fileSystem;
        private final Location location;
        private final long fileLength;
        private final long offset;
        private final long length;

        public Split(
                TrinoFileSystem fileSystem,
                Location location,
                long fileLength,
                long offset,
                long length)
        {
            this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
            this.location = requireNonNull(location, "location is null");
            this.fileLength = fileLength;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public Long call()
                throws Exception
        {
            try (ConnectorPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                    fileSystem.newInputFile(location),
                    offset,
                    length,
                    List.of(new HiveColumnHandle("SearchPhrase", 0, HIVE_STRING, VARCHAR, Optional.empty(), REGULAR, Optional.empty())),
                    List.of(TupleDomain.all()),
                    true,
                    DateTimeZone.UTC,
                    new FileFormatDataSourceStats(),
                    new ParquetReaderConfig().toParquetReaderOptions(),
                    Optional.empty(),
                    Optional.empty(),
                    new HiveConfig().getDomainCompactionThreshold(),
                    OptionalLong.of(fileLength));
                    BufferPages bufferPages = new BufferPages();
                    CopyToDevice copyToDevice = new CopyToDevice(bufferPages, List.of(VARCHAR), Set.of(0));
                    Scalar expected = Scalar.fromString("ricca full that – du have")) {
                while (!pageSource.isFinished()) {
                    SourcePage sourcePage = pageSource.getNextSourcePage();
                    if (sourcePage != null) {
                        bufferPages.addInput(sourcePage.getPage());
                    }
                }
                bufferPages.noMoreInput();

                long found = 0;
                while (true) {
                    @Own Result next = copyToDevice.execute();
                    switch (next) {
                        case Blocked(_) -> {
                            throw new UnsupportedOperationException("blocked");
                        }
                        case GpuOperation.Data(GpuPage page) -> {
                            try (page) {
                                switch (page.column(0)) {
                                    case Column.Blocks _ -> {
                                        throw new UnsupportedOperationException("blocks");
                                    }
                                    case Column.DeviceMemory deviceMemory -> {
                                        try (ColumnVector columnVector = deviceMemory.columnVector().equalToNullAware(expected)) {
                                            try (Scalar sum = columnVector.sum(DType.INT64)) {
                                                found += sum.getLong();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        case GpuOperation.Finished() -> {
                            return found;
                        }
                        case GpuOperation.Yielded() -> {
                            continue;
                        }
                    }
                }
            }
        }
    }
}
