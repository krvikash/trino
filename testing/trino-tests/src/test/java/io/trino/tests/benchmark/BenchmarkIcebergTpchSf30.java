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

/**
 * Iceberg TPC-H entry point at scale factor 30.
 */
public final class BenchmarkIcebergTpchSf30
{
    private BenchmarkIcebergTpchSf30() {}

    static void main(String[] args)
            throws Exception
    {
        System.exit(BenchmarkRunner.run(args, new IcebergTpchSf30Workload(), BenchmarkIcebergTpchSf30.class));
    }

    static final class IcebergTpchSf30Workload
            extends BaseIcebergTpchWorkload
    {
        IcebergTpchSf30Workload()
        {
            super(30);
        }

        @Override
        public DataSize jvmHeapSize()
        {
            return DataSize.of(30, DataSize.Unit.GIGABYTE);
        }

        @Override
        public void validateDataLocation(String dataLocation)
        {
            super.validateDataLocation(dataLocation);
            String expected =
                    """
                    1073011159 lineitem-0e2da0fa760040d4b1ff04da71924bba/data/20260605_113217_00003_rtazt-43feb39a-c0e3-4db3-a021-0f3068249306.parquet
                    1073063113 lineitem-0e2da0fa760040d4b1ff04da71924bba/data/20260605_113217_00003_rtazt-a98b64cf-8f64-448a-873c-8d90273d0c94.parquet
                    1073113541 lineitem-0e2da0fa760040d4b1ff04da71924bba/data/20260605_113217_00003_rtazt-b1e559fa-49e9-436a-a923-cbeafb1a7d8f.parquet
                    1073194749 lineitem-0e2da0fa760040d4b1ff04da71924bba/data/20260605_113217_00003_rtazt-b5fa6c0a-6dda-4d34-bb25-5384641006fe.parquet
                    1073199692 lineitem-0e2da0fa760040d4b1ff04da71924bba/data/20260605_113217_00003_rtazt-7d426d05-9d94-439e-bfb4-1cace3d3ece9.parquet
                    1073247022 lineitem-0e2da0fa760040d4b1ff04da71924bba/data/20260605_113217_00003_rtazt-99323fc6-bf0a-421d-895a-8fbeaf1ea3dc.parquet
                    1073700444 orders-e0bce2fbd20f49a5be1240076de42f58/data/20260605_113151_00002_rtazt-ba6af26f-e480-4c06-8969-6169595b7144.parquet
                    1074615352 partsupp-60e773e49a6d499da12323f8c550264d/data/20260605_113444_00005_rtazt-f87dd9b0-1e90-46c0-bfab-0e54e1df67d3.parquet
                    128068 part-8bff82460e214a10b00b90a29cb105ff/metadata/20260605_113440_00004_rtazt-269b8aa9-282a-4184-9444-ad8cc51d7481.stats
                    157018 partsupp-60e773e49a6d499da12323f8c550264d/metadata/20260605_113444_00005_rtazt-835b06c5-e16d-4cd1-9f1a-21aa64332663.stats
                    1583 nation-dc9e1d142aa840bdad5e2fc7ccb4a887/metadata/20260605_113455_00007_rtazt-e2e05c4a-73d8-40c8-b5e8-7dd215f544f7.stats
                    172787 orders-e0bce2fbd20f49a5be1240076de42f58/metadata/20260605_113151_00002_rtazt-05e90347-423f-44df-b983-ce5e048b2408.stats
                    182983021 partsupp-60e773e49a6d499da12323f8c550264d/data/20260605_113444_00005_rtazt-baf84080-f7af-475e-9ae2-f4db7d8a045d.parquet
                    185487 customer-deb77f6e7ac64ac49be8806cd9f795b7/metadata/20260605_113146_00001_rtazt-7ebf7dd2-3990-4015-b153-28d59ae63c0b.stats
                    186629917 part-8bff82460e214a10b00b90a29cb105ff/data/20260605_113440_00004_rtazt-1aa8c5de-c6b1-4770-9c67-08fe4b927998.parquet
                    189847 supplier-ae961b8cec9b4dfba2bde399e980121e/metadata/20260605_113455_00006_rtazt-dfaa0d88-5d5b-4745-bd1a-31582bff328f.stats
                    215012 lineitem-0e2da0fa760040d4b1ff04da71924bba/metadata/20260605_113217_00003_rtazt-caea70b8-f2c4-47c2-9741-52c5ff9da50d.stats
                    2329 nation-dc9e1d142aa840bdad5e2fc7ccb4a887/data/20260605_113455_00007_rtazt-7f5490fd-099d-4fd0-bd06-b71719790b94.parquet
                    23671895 supplier-ae961b8cec9b4dfba2bde399e980121e/data/20260605_113455_00006_rtazt-3f3cc178-737e-4496-bb58-ed828a74205f.parquet
                    2420 region-73e9211f9ddf467eb4a074d59ff6ecc8/metadata/00000-be5f85f8-0d87-4667-a2fe-9a71c5d4c312.metadata.json
                    246361884 lineitem-0e2da0fa760040d4b1ff04da71924bba/data/20260605_113217_00003_rtazt-843c4f85-bdd1-474a-a1c8-557938e6fa56.parquet
                    2619 nation-dc9e1d142aa840bdad5e2fc7ccb4a887/metadata/00000-900d5bd6-2032-4f32-94df-8f5330318408.metadata.json
                    2875 partsupp-60e773e49a6d499da12323f8c550264d/metadata/00000-adae3918-1996-4252-bbcb-2dbf4cfba323.metadata.json
                    3249 supplier-ae961b8cec9b4dfba2bde399e980121e/metadata/00000-ffb7e2a8-64ae-4d3d-9941-f2d64730ad7f.metadata.json
                    3453 customer-deb77f6e7ac64ac49be8806cd9f795b7/metadata/00000-805b7bcc-40f4-45bf-8b2b-9dad68a80bd4.metadata.json
                    3598 part-8bff82460e214a10b00b90a29cb105ff/metadata/00000-38f8d5d7-a991-4ab8-bf97-302257f7e64a.metadata.json
                    3653 orders-e0bce2fbd20f49a5be1240076de42f58/metadata/00000-1546667a-6455-40b7-9659-c4d5ea3e5209.metadata.json
                    370475540 customer-deb77f6e7ac64ac49be8806cd9f795b7/data/20260605_113146_00001_rtazt-ea0c2870-4c0c-4d51-81fc-5c4ac34e9805.parquet
                    4458 nation-dc9e1d142aa840bdad5e2fc7ccb4a887/metadata/snap-2550373765649519306-1-ad527c5e-c213-4e41-bf57-29a28198d0c3.avro
                    4459 region-73e9211f9ddf467eb4a074d59ff6ecc8/metadata/snap-1942691028942943406-1-b5a55aa1-bec2-4373-8a93-7b3f0a27c21f.avro
                    4463 part-8bff82460e214a10b00b90a29cb105ff/metadata/snap-776467903851021691-1-9aee694d-699d-4fa7-bb33-dd888a3d7bf9.avro
                    4464 orders-e0bce2fbd20f49a5be1240076de42f58/metadata/snap-3636413936841707785-1-40257e2b-2584-4e20-854e-2b60a4738407.avro
                    4467 supplier-ae961b8cec9b4dfba2bde399e980121e/metadata/snap-7935291987815948683-1-1fcccd1a-84fe-4cca-b352-9ac695c1d819.avro
                    4468 customer-deb77f6e7ac64ac49be8806cd9f795b7/metadata/snap-7321514682353725553-1-7774dc17-b104-42e9-bfc8-334a58822d82.avro
                    4470 lineitem-0e2da0fa760040d4b1ff04da71924bba/metadata/snap-7598378858913992381-1-929e10ad-88eb-4df7-a555-cebb36b0d68a.avro
                    4470 partsupp-60e773e49a6d499da12323f8c550264d/metadata/snap-7495862800700312656-1-60fcbcfc-8e61-451f-b168-59c123b08167.avro
                    5048 lineitem-0e2da0fa760040d4b1ff04da71924bba/metadata/00000-e5b25fd0-bdc4-46ab-9f25-c49d680119ff.metadata.json
                    656075771 orders-e0bce2fbd20f49a5be1240076de42f58/data/20260605_113151_00002_rtazt-71ad5c4a-7a60-4390-9fbc-b255830b83ea.parquet
                    7179 region-73e9211f9ddf467eb4a074d59ff6ecc8/metadata/b5a55aa1-bec2-4373-8a93-7b3f0a27c21f-m0.avro
                    7248 nation-dc9e1d142aa840bdad5e2fc7ccb4a887/metadata/ad527c5e-c213-4e41-bf57-29a28198d0c3-m0.avro
                    7529 partsupp-60e773e49a6d499da12323f8c550264d/metadata/60fcbcfc-8e61-451f-b168-59c123b08167-m0.avro
                    7552 supplier-ae961b8cec9b4dfba2bde399e980121e/metadata/1fcccd1a-84fe-4cca-b352-9ac695c1d819-m0.avro
                    7670 customer-deb77f6e7ac64ac49be8806cd9f795b7/metadata/7774dc17-b104-42e9-bfc8-334a58822d82-m0.avro
                    7730 part-8bff82460e214a10b00b90a29cb105ff/metadata/9aee694d-699d-4fa7-bb33-dd888a3d7bf9-m0.avro
                    7919 orders-e0bce2fbd20f49a5be1240076de42f58/metadata/40257e2b-2584-4e20-854e-2b60a4738407-m0.avro
                    843 region-73e9211f9ddf467eb4a074d59ff6ecc8/metadata/20260605_113455_00008_rtazt-1dffd5af-2d8f-4931-9e8d-53a9618f4a85.stats
                    938 region-73e9211f9ddf467eb4a074d59ff6ecc8/data/20260605_113455_00008_rtazt-de3e7509-0d0b-4426-9f19-3ffa402a8db0.parquet
                    9597 lineitem-0e2da0fa760040d4b1ff04da71924bba/metadata/929e10ad-88eb-4df7-a555-cebb36b0d68a-m0.avro
                    """;
            BenchmarkRunner.verifyDataListing(resolveTablesLocation(dataLocation), "Run `testing/benchmark-data/hydrate.sh iceberg-tpch-sf30` first.", expected);
        }
    }
}
