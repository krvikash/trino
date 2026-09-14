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

import java.nio.file.Path;
import java.util.List;

public final class BenchmarkIcebergWindow
{
    private BenchmarkIcebergWindow() {}

    static void main(String[] args)
            throws Exception
    {
        System.exit(BenchmarkRunner.run(args, new WindowWorkload(), BenchmarkIcebergWindow.class));
    }

    static final class WindowWorkload
            extends BenchmarkIcebergTpcdsSf100.TpcdsSf100Workload
    {
        @Override
        public String name()
        {
            return "iceberg-window";
        }

        @Override
        public List<String> defaultQueries()
        {
            return Window.allQueries();
        }

        @Override
        public String normalizeQuery(String query)
        {
            return Window.normalizeQuery(query);
        }

        @Override
        public String readQuery(String query)
        {
            return Window.readQuery(query, benchmarkCatalog, "tpcds");
        }

        @Override
        public String defaultDataLocation()
        {
            return Path.of(System.getProperty("user.home"), "starburst-benchmark-data", "iceberg-tpcds-sf%d".formatted(scaleFactor)).toString();
        }

        @Override
        public String expectedResultResource(String query)
        {
            return "sql/trino/window/results/%s.ndjson".formatted(query);
        }
    }
}
