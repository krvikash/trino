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
import ai.rapids.cudf.ParquetOptions;
import ai.rapids.cudf.Scalar;
import ai.rapids.cudf.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Read `SearchPhrase` column from ClickBench Parquet files on the CPU using cudf library directly.
 */
public class ClickBenchQ06TableScanCudfPure
{
    private ClickBenchQ06TableScanCudfPure() {}

    static final int WARMUPS = 10;
    static final int MEASURED = 20;

    static final int CPU_CORES = Runtime.getRuntime().availableProcessors();
    static final int THREADS = CPU_CORES * 2;
//    static final int THREADS = 1;

    static void main()
            throws Exception
    {
        System.out.println("WARMUPS = " + WARMUPS);
        System.out.println("MEASURED = " + MEASURED);

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
        int pendingSplits = 0;
        try (ExecutorService executor = Executors.newFixedThreadPool(THREADS)) {
            CompletionService<Long> completionService = new ExecutorCompletionService<>(executor);
            Path tablePath = BenchmarkClickBench.dataLocation();
            try (Stream<Path> listing = Files.list(tablePath)) {
                for (Path filePath : listing.toList()) {
                    completionService.submit(new Split(filePath));
                    pendingSplits++;
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
        private final Path filePath;

        public Split(Path filePath)
        {
            this.filePath = filePath;
        }

        @Override
        public Long call()
                throws Exception
        {
            ParquetOptions parquetOptions = ParquetOptions.builder()
                    .includeColumn("searchphrase")
                    .build();
            try (Table table = Table.readParquet(
                    parquetOptions,
                    filePath.toFile());
                    Scalar expected = Scalar.fromString("ricca full that – du have")) {
                try (ColumnVector columnVector = table.getColumn(0).equalToNullAware(expected)) {
                    try (Scalar sum = columnVector.sum(DType.INT64)) {
                        return sum.getLong();
                    }
                }
            }
        }
    }
}
