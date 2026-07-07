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

import static io.trino.tests.benchmark.IcebergTablesUtil.resolveTablesLocation;

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
                    1073644111 lineitem-55afd99516654c80a764dad11c3869d8/data/20260707_071147_00003_83s6s-945af6c5-1fe7-4313-bc35-2edcb122d30b.parquet
                    1073673769 lineitem-55afd99516654c80a764dad11c3869d8/data/20260707_071147_00003_83s6s-80730d66-a663-4e95-89c4-1fc539dede47.parquet
                    1073721809 lineitem-55afd99516654c80a764dad11c3869d8/data/20260707_071147_00003_83s6s-e4773f2b-5203-4edc-9e23-5984391268f5.parquet
                    1073745461 lineitem-55afd99516654c80a764dad11c3869d8/data/20260707_071147_00003_83s6s-ebc662c9-35d5-4bfe-8f51-f42a00a59b36.parquet
                    1073848585 orders-8ed77cfa918b4ab89ea7547c81d4c4d4/data/20260707_071113_00002_83s6s-b70d2e53-c4a1-4591-850f-ff55e46059b4.parquet
                    121881256 part-3d2ec790b47e45f991b3b2b68f393727/data/20260707_071424_00004_83s6s-9d5d1f98-5529-4d7f-929d-b6a26dc9e55e.parquet
                    128100 part-3d2ec790b47e45f991b3b2b68f393727/metadata/20260707_071424_00004_83s6s-eeffe947-dbfd-4b26-977e-e61796cee3da.stats
                    15378741 supplier-c04975f381e749eb8381a19c96174a26/data/20260707_071443_00006_83s6s-49b31e0c-d002-4dbd-86d7-908eff779cf2.parquet
                    157061 partsupp-560dc06ebfcc40798253e594b6387a67/metadata/20260707_071429_00005_83s6s-aa61401a-07eb-4e80-9ce2-c4eadb6f7bf0.stats
                    1583 nation-f18221523a134c779438d63aeeff8c9d/metadata/20260707_071443_00007_83s6s-43b17cac-4357-45d9-9968-fd761920c602.stats
                    172862 orders-8ed77cfa918b4ab89ea7547c81d4c4d4/metadata/20260707_071113_00002_83s6s-273ff33d-e6bf-47c1-8172-62c12463e129.stats
                    185889 customer-4d33c779befa4572adf9456509446417/metadata/20260707_071104_00001_83s6s-430d423c-1101-49cb-a3fa-57350ee956d6.stats
                    1889 nation-f18221523a134c779438d63aeeff8c9d/data/20260707_071443_00007_83s6s-1007b21d-ca06-43aa-b2db-2f03f5faf156.parquet
                    189984 supplier-c04975f381e749eb8381a19c96174a26/metadata/20260707_071443_00006_83s6s-b080efac-2faa-4e91-b0da-133b964d066e.stats
                    215052 lineitem-55afd99516654c80a764dad11c3869d8/metadata/20260707_071147_00003_83s6s-a7de7ae9-3e2b-4a12-94f3-a3651484bed3.stats
                    241828440 customer-4d33c779befa4572adf9456509446417/data/20260707_071104_00001_83s6s-5d011d75-bd38-43dc-baec-e86247927068.parquet
                    2422 region-57ac5ea65e314fe28d410437c08781da/metadata/00000-cd45abe1-e0f0-4a01-9373-34cb23315fc6.metadata.json
                    2621 nation-f18221523a134c779438d63aeeff8c9d/metadata/00000-ed55d603-f2fe-4ca3-bc95-2606da9e69b1.metadata.json
                    2875 partsupp-560dc06ebfcc40798253e594b6387a67/metadata/00000-204e0840-2b10-46cb-978c-42903f62462b.metadata.json
                    3251 supplier-c04975f381e749eb8381a19c96174a26/metadata/00000-6fe93f5a-24b0-4ce7-9b7e-182a342e567d.metadata.json
                    3455 customer-4d33c779befa4572adf9456509446417/metadata/00000-e2d2f6a5-4e22-48b0-9987-b94ae381fa08.metadata.json
                    3615 part-3d2ec790b47e45f991b3b2b68f393727/metadata/00000-41ce2c6d-eb09-4767-af51-41b0ae8bfcfd.metadata.json
                    3655 orders-8ed77cfa918b4ab89ea7547c81d4c4d4/metadata/00000-e8e9ffb9-d086-47c5-a587-b39d1588fa8f.metadata.json
                    433856205 lineitem-55afd99516654c80a764dad11c3869d8/data/20260707_071147_00003_83s6s-e6e80486-a685-4feb-bf45-a18bf01d1c6a.parquet
                    4459 nation-f18221523a134c779438d63aeeff8c9d/metadata/snap-9117292851670739507-1-8299b31b-7a08-4b15-8e46-ff77d169e0c4.avro
                    4460 region-57ac5ea65e314fe28d410437c08781da/metadata/snap-3591358310827781250-1-9b12de56-3a1b-4024-b647-8b7dc44b212d.avro
                    4463 part-3d2ec790b47e45f991b3b2b68f393727/metadata/snap-3672069282300900374-1-4e18029c-25a6-4283-b200-8b06a3321743.avro
                    4467 orders-8ed77cfa918b4ab89ea7547c81d4c4d4/metadata/snap-4118338369762703656-1-c5233577-6eca-4bf1-ade9-b041efc53827.avro
                    4468 customer-4d33c779befa4572adf9456509446417/metadata/snap-8470417472340873681-1-b56fd709-bdc2-4759-8b3b-1972996e060b.avro
                    4468 partsupp-560dc06ebfcc40798253e594b6387a67/metadata/snap-1526480340393150765-1-143f8419-45d4-4707-9b8e-98c45647b5e9.avro
                    4468 supplier-c04975f381e749eb8381a19c96174a26/metadata/snap-1049277659653875482-1-0e75eaf0-83f8-4dda-bd1d-2fd75a8fcf86.avro
                    4470 lineitem-55afd99516654c80a764dad11c3869d8/metadata/snap-6188881881144341221-1-b995fbd9-0c9f-40bf-8e65-99cd3089ed22.avro
                    5050 lineitem-55afd99516654c80a764dad11c3869d8/metadata/00000-71016571-f117-4d89-97f5-dfc39314c1ca.metadata.json
                    7179 region-57ac5ea65e314fe28d410437c08781da/metadata/9b12de56-3a1b-4024-b647-8b7dc44b212d-m0.avro
                    7250 nation-f18221523a134c779438d63aeeff8c9d/metadata/8299b31b-7a08-4b15-8e46-ff77d169e0c4-m0.avro
                    7390 partsupp-560dc06ebfcc40798253e594b6387a67/metadata/143f8419-45d4-4707-9b8e-98c45647b5e9-m0.avro
                    7545 supplier-c04975f381e749eb8381a19c96174a26/metadata/0e75eaf0-83f8-4dda-bd1d-2fd75a8fcf86-m0.avro
                    7667 customer-4d33c779befa4572adf9456509446417/metadata/b56fd709-bdc2-4759-8b3b-1972996e060b-m0.avro
                    7722 part-3d2ec790b47e45f991b3b2b68f393727/metadata/4e18029c-25a6-4283-b200-8b06a3321743-m0.avro
                    7900 orders-8ed77cfa918b4ab89ea7547c81d4c4d4/metadata/c5233577-6eca-4bf1-ade9-b041efc53827-m0.avro
                    79973164 orders-8ed77cfa918b4ab89ea7547c81d4c4d4/data/20260707_071113_00002_83s6s-b083920d-ea8d-4f85-844b-17fe66c5abb8.parquet
                    835213392 partsupp-560dc06ebfcc40798253e594b6387a67/data/20260707_071429_00005_83s6s-12fcb803-2518-4018-9cdd-7d9b5822b5e5.parquet
                    843 region-57ac5ea65e314fe28d410437c08781da/metadata/20260707_071443_00008_83s6s-56f2b1b8-b8f8-4510-8c71-5a26df3a7f38.stats
                    874 region-57ac5ea65e314fe28d410437c08781da/data/20260707_071443_00008_83s6s-21ac095e-d43b-44bc-9276-3005c10f353f.parquet
                    9175 lineitem-55afd99516654c80a764dad11c3869d8/metadata/b995fbd9-0c9f-40bf-8e65-99cd3089ed22-m0.avro
                    """;
            BenchmarkRunner.verifyDataListing(resolveTablesLocation(dataLocation), "Run `testing/benchmark-data/hydrate.sh iceberg-tpch-sf30` first.", expected);
        }
    }
}
