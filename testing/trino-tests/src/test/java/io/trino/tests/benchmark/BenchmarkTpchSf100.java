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

import io.airlift.units.DataSize;

import java.nio.file.Path;

/**
 * TPC-H entry point at scale factor 100.
 */
public final class BenchmarkTpchSf100
{
    private BenchmarkTpchSf100() {}

    static void main(String[] args)
            throws Exception
    {
        System.exit(BenchmarkRunner.run(args, new TpchSf100Workload(), BenchmarkTpchSf100.class));
    }

    static final class TpchSf100Workload
            extends BaseTpchWorkload
    {
        TpchSf100Workload()
        {
            super(100);
        }

        @Override
        public String profileInterval()
        {
            // SF100 queries are long enough that 10 ms still gives 1k+ samples per query, and
            // the lower SIGPROF rate keeps macOS' itimer alive across the whole suite (5 ms
            // collapses partway through). SF30 stays at the harness default (5 ms) — its
            // shorter queries need the finer resolution and don't accumulate enough cumulative
            // profiler time to hit the itimer ceiling.
            return "10ms";
        }

        @Override
        public DataSize jvmHeapSize()
        {
            return DataSize.of(80, DataSize.Unit.GIGABYTE);
        }

        @Override
        public void validateDataLocation(String dataLocation)
        {
            super.validateDataLocation(dataLocation);
            String expected = """
                              1067107265 lineitem/20260428_102244_00008_kgfxs_795ff063-4f05-4bb5-8100-551c6d38fe78
                              1067333024 lineitem/20260428_102244_00008_kgfxs_019ef8cf-b329-4835-bf91-5a2ac162274d
                              1067356263 lineitem/20260428_102244_00008_kgfxs_bc664050-e74b-4063-b6a0-ed4c0915c2c9
                              1067381473 lineitem/20260428_102244_00008_kgfxs_f969f72b-58b8-48d5-bbe4-e712ef4888f5
                              1067559186 lineitem/20260428_102244_00008_kgfxs_44fad43c-647c-4cc7-9759-6e2da7358493
                              1067570636 lineitem/20260428_102244_00008_kgfxs_45723022-ce39-4040-a284-1dbd4d235586
                              1067586702 lineitem/20260428_102244_00008_kgfxs_003c7a7b-c6d8-4a37-98e6-bd99044cbd30
                              1067597097 lineitem/20260428_102244_00008_kgfxs_198a9cb5-ab3b-41d9-b606-6333391c4ea8
                              1067610490 lineitem/20260428_102244_00008_kgfxs_26891869-eae5-4807-8fa7-d7719e7c10f1
                              1067657436 lineitem/20260428_102244_00008_kgfxs_6f0b806b-f0c1-4708-9ebc-e7f7be938cbe
                              1067734800 lineitem/20260428_102244_00008_kgfxs_7245a22c-3a6e-4968-860d-c6c2f32250e4
                              1067764484 lineitem/20260428_102244_00008_kgfxs_dce5872c-86a2-4eaf-8738-09d6dd478fd3
                              1067856301 lineitem/20260428_102244_00008_kgfxs_a3991173-a76c-4c37-a4e7-59156aff39b6
                              1068041603 lineitem/20260428_102244_00008_kgfxs_ae7ab80b-1dd3-49b3-9841-3ab38ac51bc5
                              1068064253 lineitem/20260428_102244_00008_kgfxs_a4202332-98db-4c61-9f07-ed4699bad01a
                              1068130303 lineitem/20260428_102244_00008_kgfxs_d2e23d65-c57d-4d88-854f-62946ca0f762
                              1068500415 lineitem/20260428_102244_00008_kgfxs_7f3ba9d8-5fd6-4410-a78c-98123146e5be
                              1068570731 lineitem/20260428_102244_00008_kgfxs_b4e1ba00-b10d-438f-9aae-09f9df3f9160
                              1069611477 lineitem/20260428_102244_00008_kgfxs_ecf1bf1c-d6b2-4c72-aabe-11ee7ddeda9c
                              1069647806 lineitem/20260428_102244_00008_kgfxs_6528411a-a78c-4e36-8e5a-fee040a06b7a
                              1069929382 lineitem/20260428_102244_00008_kgfxs_5cba70e7-8864-4e2c-907a-a42958934155
                              1071465456 customer/20260428_101928_00003_kgfxs_486c745a-c55b-4029-b9d2-7962e753166c
                              1072026363 orders/20260428_102057_00007_kgfxs_9e4faea3-4257-437f-9e3b-e4f3820ef7ae
                              1072353575 orders/20260428_102057_00007_kgfxs_576b2130-f647-4839-a358-df3d18be1a6e
                              1072376023 orders/20260428_102057_00007_kgfxs_63825cb4-0287-48c0-a171-a5c853faec6f
                              1072382015 orders/20260428_102057_00007_kgfxs_2726295d-2867-4aad-a528-9aed1e6f82d3
                              1072383348 orders/20260428_102057_00007_kgfxs_919c6d1f-b8e5-48d9-9c0c-81c23a4aaab7
                              1072427402 orders/20260428_102057_00007_kgfxs_e564f031-4302-4c3a-b0a1-a9e8aca7779f
                              1073731934 partsupp/20260428_102008_00006_kgfxs_30fc8d08-46b7-4917-b651-926e7034a31f
                              1073784425 partsupp/20260428_102008_00006_kgfxs_e5b5210b-5d21-4273-ae1f-e1a82d47154a
                              1074123482 partsupp/20260428_102008_00006_kgfxs_92887cb9-eac7-4f75-a8f3-bb2fb8b56f37
                              115366761 lineitem/20260428_102244_00008_kgfxs_b45e2f3c-3f54-4d92-b698-c8b39a6141cd
                              158423344 customer/20260428_101928_00003_kgfxs_fc906e4d-c26c-4f9b-9fd9-464f08f9a742
                              18902370 orders/20260428_102057_00007_kgfxs_3b7e52a3-e9f3-4d99-bffd-9795c7ccd969
                              2323 nation/20260428_101928_00002_kgfxs_717a22b9-572e-4881-8946-a8395e81a6be
                              610556554 part/20260428_101948_00005_kgfxs_acc2c2ca-c87d-4df2-b6f8-a32a8a0c38a6
                              78555017 supplier/20260428_101947_00004_kgfxs_720a470c-55ce-4ff6-8b8f-5821c25aae9b
                              961515270 partsupp/20260428_102008_00006_kgfxs_5719b063-0178-4241-8821-d88d0fbb4321
                              975 region/20260428_101924_00001_kgfxs_1536a9ae-0ff5-4265-9927-f00764c4a445
                              """;
            BenchmarkRunner.verifyDataListing(Path.of(dataLocation), "Run `testing/benchmark-data/hydrate.sh tpch-sf100` first.", expected);
        }
    }

    public static class CpuBenchmark
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"run", "--mode", "CPU"},
                    new TpchSf100Workload(),
                    BenchmarkTpchSf100.class));
        }
    }

    public static class GpuBenchmark
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"run", "--mode", "GPU"},
                    new TpchSf100Workload(),
                    BenchmarkTpchSf100.class));
        }
    }

    public static class Generate
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"generate"},
                    new TpchSf100Workload(),
                    BenchmarkTpchSf100.class));
        }
    }

    public static class Record
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"record"},
                    new TpchSf100Workload(),
                    BenchmarkTpchSf100.class));
        }
    }
}
