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

/**
 * TPC-H entry point at scale factor 30.
 */
public final class BenchmarkTpchSf30
{
    private BenchmarkTpchSf30() {}

    static void main(String[] args)
            throws Exception
    {
        System.exit(BenchmarkRunner.run(args, new TpchSf30Workload(), BenchmarkTpchSf30.class));
    }

    static final class TpchSf30Workload
            extends BaseTpchWorkload
    {
        TpchSf30Workload()
        {
            super(30);
        }

        @Override
        public void validateDataLocation(Path dataLocation)
        {
            String expected = """
                              1067740789 lineitem/20260429_165051_00008_ghg4b_03a96dbd-4363-4360-a500-c64bcacf0a0d
                              1068163526 lineitem/20260429_165051_00008_ghg4b_63f31b64-13ee-481c-9754-ec2f6a818d0c
                              1068263830 lineitem/20260429_165051_00008_ghg4b_7b294725-588d-4111-95c6-5bc0f88b006b
                              1068398721 lineitem/20260429_165051_00008_ghg4b_3adaa381-97b0-4da5-a199-f0cd14e3c956
                              1068400506 lineitem/20260429_165051_00008_ghg4b_147be8cd-6fae-4168-b5e1-a3b1f830dcc6
                              1068569550 lineitem/20260429_165051_00008_ghg4b_39bb0542-5b2a-4cef-a4a0-2ec5adedddc8
                              1071596716 orders/20260429_165014_00007_ghg4b_cc1de195-6675-4ef0-97aa-21589735cb58
                              1073476859 partsupp/20260429_164956_00006_ghg4b_3b783450-774e-4353-aac4-eb6e11947361
                              181153047 partsupp/20260429_164956_00006_ghg4b_cb1bda08-2f54-4952-993d-78d3a8f4a17d
                              185238346 part/20260429_164950_00005_ghg4b_022de90f-70ce-4bac-97bd-40e5fb3ea0a1
                              2323 nation/20260429_164941_00002_ghg4b_ffee5717-c53f-4c99-93e8-5bc1ffa868b8
                              23580809 supplier/20260429_164949_00004_ghg4b_9fb96e4d-e37f-48dd-9159-bc52a5c408b8
                              260789258 lineitem/20260429_165051_00008_ghg4b_0a9b2ae0-1e49-42d4-933d-db10c2604b4e
                              368955756 customer/20260429_164942_00003_ghg4b_8028a30d-902b-433f-93d0-86e43242b921
                              653489379 orders/20260429_165014_00007_ghg4b_0de07670-c9fb-4cc2-932d-8243ea615567
                              975 region/20260429_164938_00001_ghg4b_7a4a7ea1-967e-44b8-aeb7-7c60cd4a1ba0
                              """;
            BenchmarkRunner.verifyDataListing(dataLocation, "Run testing/benchmark-data/hydrate.sh first.", expected);
        }
    }

    public static class CpuBenchmark
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"run", "--mode", "CPU", "-w", "3", "-r", "6"},
                    new TpchSf30Workload(),
                    BenchmarkTpchSf30.class));
        }
    }

    public static class GpuBenchmark
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"run", "--mode", "GPU", "-w", "3", "-r", "6"},
                    new TpchSf30Workload(),
                    BenchmarkTpchSf30.class));
        }
    }

    public static class Generate
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"generate"},
                    new TpchSf30Workload(),
                    BenchmarkTpchSf30.class));
        }
    }
}
