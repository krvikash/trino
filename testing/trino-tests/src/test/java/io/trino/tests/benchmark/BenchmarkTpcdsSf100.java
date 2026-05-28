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
 * TPC-DS entry point at scale factor 100.
 */
public final class BenchmarkTpcdsSf100
{
    private BenchmarkTpcdsSf100() {}

    static void main(String[] args)
            throws Exception
    {
        System.exit(BenchmarkRunner.run(args, new TpcdsSf100Workload(), BenchmarkTpcdsSf100.class));
    }

    static final class TpcdsSf100Workload
            extends BaseTpcdsWorkload
    {
        TpcdsSf100Workload()
        {
            super(100);
        }

        @Override
        public String profileInterval()
        {
            // Matches BenchmarkTpchSf100: SF100 queries are long enough that 10 ms still gives
            // 1k+ samples per query, and the lower SIGPROF rate keeps macOS' itimer alive across
            // the whole suite (5 ms collapses partway through).
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
            String expected =
                    """
                    1073428466 inventory/20260528_112220_00011_cwgbm_0399ddcb-8c43-4712-89a3-936cdc804815
                    1073674434 web_sales/20260528_113727_00023_cwgbm_fc0744e7-a1bb-47ed-aa65-ce77cddb04f1
                    1073749973 catalog_returns/20260528_111232_00003_cwgbm_b777b4c5-10d4-4a57-ab4f-791056bcf38a
                    1073938952 web_sales/20260528_113727_00023_cwgbm_4679cb83-70b5-49a0-9109-c71f3eb23128
                    1073975292 web_sales/20260528_113727_00023_cwgbm_fb6a8764-f596-4ba5-8777-3d2c11a5e510
                    1073994495 web_sales/20260528_113727_00023_cwgbm_cf4ebe04-95f1-4a28-8791-16dfe8ca6ecc
                    1074922020 catalog_sales/20260528_111330_00004_cwgbm_321d16de-dd6b-447d-932a-b26a132cdfe5
                    1075496402 store_returns/20260528_112338_00017_cwgbm_6154acbd-c073-49be-b85e-ebb7e5c9fd05
                    1075716219 catalog_sales/20260528_111330_00004_cwgbm_d3308df6-6367-4024-9ff7-c2ba0aa34501
                    1075760407 catalog_sales/20260528_111330_00004_cwgbm_b617315d-6535-45c9-bc94-7020d4816afb
                    1076171322 store_sales/20260528_112509_00018_cwgbm_f74bc319-6a0e-433d-b2e7-2acfaf87eb92
                    1076296598 store_sales/20260528_112509_00018_cwgbm_1a993a0a-beef-4706-8f50-861b5983bd54
                    1076725054 store_sales/20260528_112509_00018_cwgbm_befb3c10-035f-438e-9f9f-f451de5bfa23
                    1076744397 store_sales/20260528_112509_00018_cwgbm_d82d1d4a-59be-41a6-9324-34f73121b993
                    1076817042 store_sales/20260528_112509_00018_cwgbm_98456c83-9f20-4b7b-b372-0b79e71f7fb9
                    1076852683 store_sales/20260528_112509_00018_cwgbm_2435c1e6-b814-45cf-ade5-d252990bd49d
                    1076926381 store_sales/20260528_112509_00018_cwgbm_c842875c-3eb0-4756-880e-58064e9dcfe3
                    1076942460 store_sales/20260528_112509_00018_cwgbm_e59035c2-598c-4bb3-88cd-9a3534070484
                    1076998928 store_sales/20260528_112509_00018_cwgbm_3388d562-d109-4f87-96d7-f6fd16a04211
                    1077009988 store_sales/20260528_112509_00018_cwgbm_4ea0f43d-c576-4c16-9aae-d0900ee56434
                    1077025968 store_sales/20260528_112509_00018_cwgbm_35be55c5-9ce9-452a-a3fd-27570f5ee9f0
                    1077040070 store_sales/20260528_112509_00018_cwgbm_15e486d9-e82f-489d-bb0f-2d3f9709fbd2
                    1077135587 catalog_sales/20260528_111330_00004_cwgbm_be2eaddb-2da5-41e5-9a15-8a1565f3816c
                    1077194107 catalog_sales/20260528_111330_00004_cwgbm_1e88ad83-cdcd-40b0-b2b5-028462da7291
                    1077372501 catalog_sales/20260528_111330_00004_cwgbm_1df82853-e7d3-4c73-9699-45287132a592
                    1077572886 catalog_sales/20260528_111330_00004_cwgbm_3bd35893-ec38-407e-94c8-ff98b0295bae
                    1078089124 catalog_sales/20260528_111330_00004_cwgbm_d81ce2d2-4d75-4d9b-ae2c-bf2810059353
                    1078160643 catalog_sales/20260528_111330_00004_cwgbm_3fafcc3e-c0d1-49c9-ad72-2ec5cb38d5b3
                    109400948 customer/20260528_112211_00005_cwgbm_942d2d81-a839-4cd6-acf3-d7e8c3cbc9c7
                    1128464 time_dim/20260528_113700_00019_cwgbm_1eac7a05-f074-4abf-b8e8-1736e2d29351
                    1186889 catalog_page/20260528_111232_00002_cwgbm_a5765a7e-99f1-4181-97a9-4b7323ab43cf
                    166290836 store_sales/20260528_112509_00018_cwgbm_a597b589-9c5b-4ef1-ba6c-b89a6b897e69
                    1690 reason/20260528_112338_00014_cwgbm_f14b4a4c-1bb8-446e-bbf5-13f97c368a66
                    1831 ship_mode/20260528_112338_00015_cwgbm_7050a607-36f5-486c-92b0-2904815aa8f1
                    1839497 date_dim/20260528_112220_00008_cwgbm_c07f2411-a2a9-4c84-bc15-28b9846ba401
                    19297101 item/20260528_112335_00012_cwgbm_20cde3f0-0ac1-45d4-a750-9260e9693ce9
                    19492776 customer_address/20260528_112216_00006_cwgbm_1bb8bc9c-60a1-4782-bd5d-9d58de04626f
                    31156 household_demographics/20260528_112220_00009_cwgbm_7face860-7944-40b1-8b69-abbd904c2ff0
                    3484 warehouse/20260528_113700_00020_cwgbm_0d16bde2-4f3c-43c4-aea3-223fea6372d7
                    38678 web_page/20260528_113700_00021_cwgbm_1584067d-10a5-4045-8679-63db8de99944
                    40471 store/20260528_112338_00016_cwgbm_b9e69791-9ebf-4c37-b309-d07fb4687be0
                    521490101 web_sales/20260528_113727_00023_cwgbm_75cd24ec-dd7d-4940-9a3d-53c2cef6918f
                    53952 promotion/20260528_112338_00013_cwgbm_593aaaf0-91fa-4386-b533-a2af6faf8ebb
                    542321758 store_returns/20260528_112338_00017_cwgbm_ad83d991-e473-4fcd-baab-a518c68683e9
                    542603614 web_returns/20260528_113700_00022_cwgbm_44145b91-f159-4b76-a406-f51c67a554ec
                    543817093 catalog_sales/20260528_111330_00004_cwgbm_97101d24-eda6-4f7d-b86d-17cba7b81ada
                    7774589 customer_demographics/20260528_112218_00007_cwgbm_ef6e3264-fc68-4f6e-8e61-7c5e013550c0
                    819 income_band/20260528_112220_00010_cwgbm_9b967f15-2efb-4205-a92b-4af0fc42139c
                    8301 web_site/20260528_114135_00024_cwgbm_286064d7-c312-4324-80c3-9fff87b2c64d
                    910248870 inventory/20260528_112220_00011_cwgbm_01ba93f5-3f4b-4193-ab7d-6039fdc5f14a
                    9995 call_center/20260528_111230_00001_cwgbm_baca8512-6983-49e0-9de7-c36f7b874391
                    """;
            BenchmarkRunner.verifyDataListing(Path.of(dataLocation), "Run `testing/benchmark-data/hydrate.sh tpcds-sf100` first.", expected);
        }
    }

    public static class CpuBenchmark
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"run", "--mode", "CPU"},
                    new TpcdsSf100Workload(),
                    BenchmarkTpcdsSf100.class));
        }
    }

    public static class GpuBenchmark
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"run", "--mode", "GPU"},
                    new TpcdsSf100Workload(),
                    BenchmarkTpcdsSf100.class));
        }
    }

    public static class Generate
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"generate"},
                    new TpcdsSf100Workload(),
                    BenchmarkTpcdsSf100.class));
        }
    }

    public static class Record
    {
        static void main()
                throws Exception
        {
            System.exit(BenchmarkRunner.run(
                    new String[] {"record"},
                    new TpcdsSf100Workload(),
                    BenchmarkTpcdsSf100.class));
        }
    }
}
