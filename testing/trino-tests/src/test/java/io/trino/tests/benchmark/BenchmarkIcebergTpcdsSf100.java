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
 * Iceberg TPC-DS entry point at scale factor 100.
 */
public final class BenchmarkIcebergTpcdsSf100
{
    private BenchmarkIcebergTpcdsSf100() {}

    static void main(String[] args)
            throws Exception
    {
        System.exit(BenchmarkRunner.run(args, new TpcdsSf100Workload(), BenchmarkIcebergTpcdsSf100.class));
    }

    static final class TpcdsSf100Workload
            extends BaseIcebergTpcdsWorkload
    {
        TpcdsSf100Workload()
        {
            super(100);
        }

        @Override
        public String profileInterval()
        {
            return "10ms";
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
                    10013 call_center-d25f1f5f82b54eb4b952a5be304b7f73/data/20260605_130145_00002_st26k-ae48a393-571a-46bb-babd-2dd3013d01c7.parquet
                    103426 catalog_page-b6732c62099d4a1c951013c9052045d6/metadata/20260605_130146_00004_st26k-97576452-c624-4d62-bfbf-ce619db939dd.stats
                    1070061126 catalog_returns-256bf8aeece34579bc0f9d19b2cb4543/data/20260605_130146_00006_st26k-6f36f6bc-170c-4704-868a-9ed419791842.parquet
                    1073557632 inventory-484b80a5590e449199b01b4ca507ab91/data/20260605_130833_00022_st26k-6b2652ed-781a-4da9-8bb9-7e93df764419.parquet
                    1073901108 web_sales-28c8ab917b0a49faa78a5bfde99e515b/data/20260605_131953_00046_st26k-741029d9-9c41-4192-b5da-767532174a32.parquet
                    1073906283 web_sales-28c8ab917b0a49faa78a5bfde99e515b/data/20260605_131953_00046_st26k-ed33806c-fb97-46cb-a584-d672c84431b8.parquet
                    1073929588 web_sales-28c8ab917b0a49faa78a5bfde99e515b/data/20260605_131953_00046_st26k-c193fe6e-475f-4656-a9f3-c098d604ae61.parquet
                    1074120034 web_sales-28c8ab917b0a49faa78a5bfde99e515b/data/20260605_131953_00046_st26k-005296fa-beec-4161-9fdd-766388c3711f.parquet
                    1075637355 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-bdb71670-693a-4de0-979f-cb1075b46461.parquet
                    1075643112 store_returns-9f1669793a40476bba696a31a5da1417/data/20260605_130949_00034_st26k-71f1c5e2-7c18-4c79-9f6c-5ef0f04e2e73.parquet
                    1076161910 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-f596178c-a62c-4b43-8801-41d0cc8ca22d.parquet
                    1076200257 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-9b9b8a4e-dfab-4aa3-ba18-dd6762f9d990.parquet
                    1076228624 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-c1867951-e5da-4f46-ad45-44efb401ca2e.parquet
                    1076254767 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-a6417baf-b1e4-47ff-9569-82c7eb31bd96.parquet
                    1076276781 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-79fba45a-9a65-4ec4-b6d3-6810de3a5707.parquet
                    1076363567 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-3738ebbb-c367-43c4-92df-3b938fe39d7a.parquet
                    1076458998 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-0a4af8bc-385c-4dc5-b4ac-b75494c14e6e.parquet
                    1076521108 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-52003cbb-4cbb-4064-b9e9-8f63a86a7d37.parquet
                    1076605774 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-5c10165e-b82e-4d38-a327-a6fbda7e24a9.parquet
                    1076683848 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-2dbd0cc7-91ab-43f2-87c2-a7276d7c9ed1.parquet
                    1076859150 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-ea31cc34-d302-445d-a2f8-741322bb0773.parquet
                    1076875292 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-225cf733-8312-42cf-83f6-4fb4aafa5380.parquet
                    1076910040 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-56bfcd1e-6908-488e-ae01-8bf31a42a6ff.parquet
                    1077050367 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-235a2c19-9770-4d68-950d-95c087257b5c.parquet
                    1077055479 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-41e516ad-69bc-47d7-8677-9a702f4a52f8.parquet
                    1077268834 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-cfcd98cd-e13d-4bed-9104-8a7a5292f085.parquet
                    1077428970 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-b1aa53ef-d4ee-49cf-acf0-74218c717cb6.parquet
                    1077483098 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-c348ba5a-7c7a-4e0b-9aa2-64b088c258d0.parquet
                    1077982342 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-bf03825b-ec2d-47b7-b553-2866f293b0f0.parquet
                    1078249942 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-2989cbd6-799d-4ec7-9fd0-e4e1f705e5d7.parquet
                    110046924 customer-a274f74636104d6384c3bffa84100cbc/data/20260605_130827_00010_st26k-e8d77cf7-0d58-4abd-95f4-c56edb82612f.parquet
                    1129203 time_dim-fb84514664e8431eb0884da596ed7a08/data/20260605_131934_00038_st26k-33d7630c-2ce1-4fa0-939b-03f2af2f489b.parquet
                    11830 web_sales-28c8ab917b0a49faa78a5bfde99e515b/metadata/d6c306cd-8757-49ff-90bb-288cd139c02d-m0.avro
                    1208371 catalog_page-b6732c62099d4a1c951013c9052045d6/data/20260605_130146_00004_st26k-6b638a0a-e1a8-4e07-8c78-962910cf708b.parquet
                    1210 income_band-b2e7b325f5e640aea9dc400e82f95ba1/metadata/20260605_130833_00020_st26k-23c5e64e-33f3-446e-9928-ae270e49f81c.stats
                    13316 store_sales-c68a9755e2f7494a841fc01770d4dd67/metadata/6fbc6daf-9dce-4f44-bde8-88088bb1b3a2-m0.avro
                    14642 catalog_sales-4db5f6164de44048a84ba10c7f17589d/metadata/47fcb069-75a7-42f9-97c4-0f0aa139f8a3-m0.avro
                    162136 customer_address-651e1c68f4884fc09f4088c8e436b22c/metadata/20260605_130830_00012_st26k-bc978f76-a82f-4c32-a112-453e1f35aea5.stats
                    171004568 store_sales-c68a9755e2f7494a841fc01770d4dd67/data/20260605_131051_00036_st26k-0fe7983c-7566-4475-b541-1b0663ecbc21.parquet
                    1801 reason-5224765121ef4211952ba66770c4bfe3/data/20260605_130949_00028_st26k-0c0974e5-4c86-472a-9704-48ac15496f8f.parquet
                    1838 ship_mode-1e424ce99bc64a09b7abbd90a22a2251/data/20260605_130949_00030_st26k-b1bc68e7-db5a-4892-bf92-7fc55e443ca5.parquet
                    1852681 date_dim-66a709699883471684cd954c981e4b59/data/20260605_130833_00016_st26k-b5da8653-422a-41ea-b3e8-fc7bae5faf0d.parquet
                    19499090 customer_address-651e1c68f4884fc09f4088c8e436b22c/data/20260605_130830_00012_st26k-ee354e40-944e-45e8-a6bb-ae2b4f805664.parquet
                    19504027 item-153ad53ffd8948a4ad24ec5020485e4d/data/20260605_130947_00024_st26k-3d88bdda-7b02-4d47-9026-4523a0c0501a.parquet
                    2045 reason-5224765121ef4211952ba66770c4bfe3/metadata/20260605_130949_00028_st26k-4f8984db-87b8-4327-912d-509f445c528a.stats
                    2091 ship_mode-1e424ce99bc64a09b7abbd90a22a2251/metadata/20260605_130949_00030_st26k-e2dce3e4-d880-4edc-8bd7-06b07e2b21a2.stats
                    2443 reason-5224765121ef4211952ba66770c4bfe3/metadata/00000-237ed449-70a8-4c19-889d-915e8ce912b4.metadata.json
                    2460 income_band-b2e7b325f5e640aea9dc400e82f95ba1/metadata/00000-31cfcbb2-70b9-4867-9658-b6562724396f.metadata.json
                    261011 item-153ad53ffd8948a4ad24ec5020485e4d/metadata/20260605_130947_00024_st26k-d2c7d5e4-c4fd-4bb1-b75c-f31f00f3dea9.stats
                    2677 inventory-484b80a5590e449199b01b4ca507ab91/metadata/00000-239d84c5-c533-41a5-96eb-00f6ed283189.metadata.json
                    28482 store-1cbeb6c68e2b46f1a61f19fe8527b57a/metadata/20260605_130949_00032_st26k-273c7a3b-cfc5-4f38-b65e-e5a76f60102f.stats
                    2897 household_demographics-1b88bf552134431998e69a8e2112fffb/metadata/00000-168dc4be-f46c-49e5-906d-5c7cdf2b2de6.metadata.json
                    3036 ship_mode-1e424ce99bc64a09b7abbd90a22a2251/metadata/00000-19096773-d0bf-42a5-ae78-618de4392efd.metadata.json
                    31089 household_demographics-1b88bf552134431998e69a8e2112fffb/data/20260605_130833_00018_st26k-684d7843-c66e-4f27-8edb-4f239b096afe.parquet
                    312138 date_dim-66a709699883471684cd954c981e4b59/metadata/20260605_130833_00016_st26k-aad53660-0cb5-4e96-a629-1958e327fb99.stats
                    318939 customer-a274f74636104d6384c3bffa84100cbc/metadata/20260605_130827_00010_st26k-809317d7-5453-4341-8fd9-ff9d56c0ddd8.stats
                    32832 customer_demographics-eef2e095d6d14945af22cef0988ed5ff/metadata/20260605_130832_00014_st26k-9e8022c6-1452-46af-8778-5921aa87b153.stats
                    34281 household_demographics-1b88bf552134431998e69a8e2112fffb/metadata/20260605_130833_00018_st26k-7218aff0-6fcb-4fc2-877c-0267f703e647.stats
                    3492 warehouse-bd781304580642779ffa8233b1fe60da/data/20260605_131935_00040_st26k-9e952a4c-3d0b-41db-b631-066acd08ba98.parquet
                    3687 catalog_page-b6732c62099d4a1c951013c9052045d6/metadata/00000-8cc2831d-6045-49d2-aec1-edb07e8563ee.metadata.json
                    3711 customer_demographics-eef2e095d6d14945af22cef0988ed5ff/metadata/00000-533e47f6-caff-4b08-9403-67e03cbab814.metadata.json
                    3801 time_dim-fb84514664e8431eb0884da596ed7a08/metadata/00000-2b5fa5bc-4bdc-422d-9cf4-9c27c53497b7.metadata.json
                    38963 web_page-ea1d765ebcfb448fa43ccb7e57e8645c/data/20260605_131935_00042_st26k-40d7400b-ceae-49ec-b6de-943edc59e09e.parquet
                    41312 store-1cbeb6c68e2b46f1a61f19fe8527b57a/data/20260605_130949_00032_st26k-983887aa-4a23-4d31-af8f-a5233b94be5b.parquet
                    42819 inventory-484b80a5590e449199b01b4ca507ab91/metadata/20260605_130833_00022_st26k-74f5e50f-2a9b-498c-a9f9-062d26aa4a28.stats
                    4322 warehouse-bd781304580642779ffa8233b1fe60da/metadata/20260605_131935_00040_st26k-d12a99df-7e50-4167-8576-c48f701d0985.stats
                    44582 web_page-ea1d765ebcfb448fa43ccb7e57e8645c/metadata/20260605_131935_00042_st26k-80c3f532-9a61-4af7-92d1-7792a4102e0c.stats
                    4461 reason-5224765121ef4211952ba66770c4bfe3/metadata/snap-4776786503880537105-1-269b2c57-fdb3-4267-8379-025103683741.avro
                    4461 store-1cbeb6c68e2b46f1a61f19fe8527b57a/metadata/snap-8639293874964219529-1-75fa4b71-4280-4c1a-b971-0aed3bd71561.avro
                    4463 call_center-d25f1f5f82b54eb4b952a5be304b7f73/metadata/snap-2007436363310103966-1-c93d0c30-21a1-4b1c-9e07-5a3ae46973a0.avro
                    4463 income_band-b2e7b325f5e640aea9dc400e82f95ba1/metadata/snap-3465247508347039407-1-5ee3d701-946b-43e9-b866-848ea7aff576.avro
                    4463 ship_mode-1e424ce99bc64a09b7abbd90a22a2251/metadata/snap-2247054207449494744-1-783714ca-7422-4355-a824-6e98e0727de8.avro
                    4463 web_site-c3dcd5b06e974e2b9b7a7182afd00678/metadata/snap-257641130186521241-1-611d5d70-b023-4136-98c4-c585b1ee9174.avro
                    4464 customer-a274f74636104d6384c3bffa84100cbc/metadata/snap-1331996744282504011-1-28e7daa7-9145-4bc9-99f8-f22c1bbefd16.avro
                    4465 item-153ad53ffd8948a4ad24ec5020485e4d/metadata/snap-8917223816282589538-1-1015534d-3828-464a-9d10-d69f4b797be1.avro
                    4465 warehouse-bd781304580642779ffa8233b1fe60da/metadata/snap-2781115315233242010-1-e5b264a3-c2cd-413d-9126-db0dc7a3b8ae.avro
                    4468 catalog_page-b6732c62099d4a1c951013c9052045d6/metadata/snap-6241879212742042886-1-31d35b21-1e1e-4c76-bf8c-cde6ac1a281a.avro
                    4468 date_dim-66a709699883471684cd954c981e4b59/metadata/snap-6495906636125945924-1-f38d7481-2ba4-4106-ac98-8764a1bf0cdd.avro
                    4468 promotion-226aa472cc894da787dfb0682638f10b/metadata/snap-4553008689569283050-1-4e173977-b4d7-43f9-bfc6-ee7f980becda.avro
                    4468 web_page-ea1d765ebcfb448fa43ccb7e57e8645c/metadata/snap-5484967225437180978-1-3aa2f431-48e1-47ce-be79-25eb14aa32ef.avro
                    4469 time_dim-fb84514664e8431eb0884da596ed7a08/metadata/snap-8487929606585862955-1-9dd93b4e-9b1f-40cf-bcc6-5aa8ca283367.avro
                    4470 inventory-484b80a5590e449199b01b4ca507ab91/metadata/snap-545628029565209427-1-19fb2067-aa27-4455-868e-759bbfc4d2db.avro
                    4471 customer_address-651e1c68f4884fc09f4088c8e436b22c/metadata/snap-3088425152003326817-1-b09a04f1-791e-4fe5-b02d-597a7f3361e3.avro
                    4471 web_sales-28c8ab917b0a49faa78a5bfde99e515b/metadata/snap-8596152157089573666-1-d6c306cd-8757-49ff-90bb-288cd139c02d.avro
                    4473 catalog_returns-256bf8aeece34579bc0f9d19b2cb4543/metadata/snap-8954657851186454704-1-8a781bc0-9f01-4e61-9ef6-f39a7e0ee41a.avro
                    4473 store_returns-9f1669793a40476bba696a31a5da1417/metadata/snap-1048793739949241640-1-fee585f8-0def-40af-986f-9d9842ae6334.avro
                    4474 catalog_sales-4db5f6164de44048a84ba10c7f17589d/metadata/snap-8947827002199304413-1-47fcb069-75a7-42f9-97c4-0f0aa139f8a3.avro
                    4474 store_sales-c68a9755e2f7494a841fc01770d4dd67/metadata/snap-5036979639539716885-1-6fbc6daf-9dce-4f44-bde8-88088bb1b3a2.avro
                    4474 web_returns-a227bf7382cb46f0827f84d7c4f65b29/metadata/snap-8631508264952853881-1-99df06ec-9708-41af-874a-52aad4ce02e3.avro
                    4476 household_demographics-1b88bf552134431998e69a8e2112fffb/metadata/snap-8958845739366110598-1-6f422339-5fd1-45e7-889a-bef3f0be90c8.avro
                    4477 customer_demographics-eef2e095d6d14945af22cef0988ed5ff/metadata/snap-2303882660515006132-1-5b55fbd9-aecd-45e5-af3c-462156be977b.avro
                    4495 customer_address-651e1c68f4884fc09f4088c8e436b22c/metadata/00000-a00184cf-985c-44ba-bcd7-402c93f843ec.metadata.json
                    45171 promotion-226aa472cc894da787dfb0682638f10b/metadata/20260605_130949_00026_st26k-2a726cba-3679-4578-ba17-bf7714d419b0.stats
                    4627 warehouse-bd781304580642779ffa8233b1fe60da/metadata/00000-f6ee0104-563f-4e59-b646-0a5e831d5426.metadata.json
                    4641 web_page-ea1d765ebcfb448fa43ccb7e57e8645c/metadata/00000-66f761c1-cb62-47e2-9ec5-3a135cdaeafc.metadata.json
                    522188733 catalog_sales-4db5f6164de44048a84ba10c7f17589d/data/20260605_130226_00008_st26k-e91f6699-0651-4465-a621-7c4bfeba6df5.parquet
                    522325469 web_sales-28c8ab917b0a49faa78a5bfde99e515b/data/20260605_131953_00046_st26k-64548cd9-5274-4e66-ab08-ff28f83bcc68.parquet
                    523573 store_returns-9f1669793a40476bba696a31a5da1417/metadata/20260605_130949_00034_st26k-eea6c0ed-5a38-4e12-b030-4c08ed829e9e.stats
                    542881490 store_returns-9f1669793a40476bba696a31a5da1417/data/20260605_130949_00034_st26k-5f870226-ff56-4e6b-b61d-b00e0b6a7c0f.parquet
                    542948367 web_returns-a227bf7382cb46f0827f84d7c4f65b29/data/20260605_131935_00044_st26k-d6361b71-2443-4800-8b24-145257d0ca19.parquet
                    54951 promotion-226aa472cc894da787dfb0682638f10b/data/20260605_130949_00026_st26k-11f4b602-ac14-4651-8eb9-0b7d1e38783b.parquet
                    5515 customer-a274f74636104d6384c3bffa84100cbc/metadata/00000-c1da9b68-93b6-44c6-a875-f490c25a7d46.metadata.json
                    5653 promotion-226aa472cc894da787dfb0682638f10b/metadata/00000-5aa01d86-72a8-4a18-a4d1-7d791a73d441.metadata.json
                    6014 store_returns-9f1669793a40476bba696a31a5da1417/metadata/00000-02af3b62-d676-4135-b656-47fd16f9ed92.metadata.json
                    6225 item-153ad53ffd8948a4ad24ec5020485e4d/metadata/00000-4ea093f6-322a-4a92-8fa5-72356e047d37.metadata.json
                    625072 store_sales-c68a9755e2f7494a841fc01770d4dd67/metadata/20260605_131051_00036_st26k-df7df15f-77da-48ba-a9c1-43f0feb67e84.stats
                    6651 store_sales-c68a9755e2f7494a841fc01770d4dd67/metadata/00000-0d098ce1-57e9-4477-9e39-d98d4ca36a84.metadata.json
                    665595 web_returns-a227bf7382cb46f0827f84d7c4f65b29/metadata/20260605_131935_00044_st26k-b9468b24-1e99-4cd4-88c5-76c99ca4dc53.stats
                    681643 catalog_returns-256bf8aeece34579bc0f9d19b2cb4543/metadata/20260605_130146_00006_st26k-77dff839-efcb-45f9-81cf-5460deca7049.stats
                    6889 web_returns-a227bf7382cb46f0827f84d7c4f65b29/metadata/00000-14d6384d-c155-4a7c-bcef-a4de35d469a9.metadata.json
                    7000 web_site-c3dcd5b06e974e2b9b7a7182afd00678/metadata/00000-b835d7e9-7f3e-40a9-802d-c44182a1a537.metadata.json
                    7170 income_band-b2e7b325f5e640aea9dc400e82f95ba1/metadata/5ee3d701-946b-43e9-b866-848ea7aff576-m0.avro
                    7188 reason-5224765121ef4211952ba66770c4bfe3/metadata/269b2c57-fdb3-4267-8379-025103683741-m0.avro
                    7351 household_demographics-1b88bf552134431998e69a8e2112fffb/metadata/6f422339-5fd1-45e7-889a-bef3f0be90c8-m0.avro
                    7370 date_dim-66a709699883471684cd954c981e4b59/metadata/00000-b97e0795-fbb7-4032-8076-4293a1725f8f.metadata.json
                    7439 ship_mode-1e424ce99bc64a09b7abbd90a22a2251/metadata/783714ca-7422-4355-a824-6e98e0727de8-m0.avro
                    7461 inventory-484b80a5590e449199b01b4ca507ab91/metadata/19fb2067-aa27-4455-868e-759bbfc4d2db-m0.avro
                    7514 catalog_returns-256bf8aeece34579bc0f9d19b2cb4543/metadata/00000-3f6de8ef-126f-4de7-8f79-2e8cb949bb40.metadata.json
                    7606 store-1cbeb6c68e2b46f1a61f19fe8527b57a/metadata/00000-6419a571-d82d-4291-9edf-fe2130782a77.metadata.json
                    7688 time_dim-fb84514664e8431eb0884da596ed7a08/metadata/9dd93b4e-9b1f-40cf-bcc6-5aa8ca283367-m0.avro
                    7695 customer_demographics-eef2e095d6d14945af22cef0988ed5ff/metadata/5b55fbd9-aecd-45e5-af3c-462156be977b-m0.avro
                    7727 catalog_page-b6732c62099d4a1c951013c9052045d6/metadata/31d35b21-1e1e-4c76-bf8c-cde6ac1a281a-m0.avro
                    7778365 customer_demographics-eef2e095d6d14945af22cef0988ed5ff/data/20260605_130832_00014_st26k-3f73da3c-bf02-4521-9e3d-799738283253.parquet
                    7930 web_site-c3dcd5b06e974e2b9b7a7182afd00678/metadata/20260605_132258_00048_st26k-8a0ca540-6dcf-49ac-b4ef-2443797f1703.stats
                    8006 call_center-d25f1f5f82b54eb4b952a5be304b7f73/metadata/00000-3e6f1f20-d866-48c4-933a-0834caa6d62a.metadata.json
                    808 income_band-b2e7b325f5e640aea9dc400e82f95ba1/data/20260605_130833_00020_st26k-8a79870b-b746-45cc-b1a3-03205c752443.parquet
                    8092 web_page-ea1d765ebcfb448fa43ccb7e57e8645c/metadata/3aa2f431-48e1-47ce-be79-25eb14aa32ef-m0.avro
                    8094 warehouse-bd781304580642779ffa8233b1fe60da/metadata/e5b264a3-c2cd-413d-9126-db0dc7a3b8ae-m0.avro
                    8098 customer_address-651e1c68f4884fc09f4088c8e436b22c/metadata/b09a04f1-791e-4fe5-b02d-597a7f3361e3-m0.avro
                    8294 web_site-c3dcd5b06e974e2b9b7a7182afd00678/data/20260605_132258_00048_st26k-08c7da34-ae29-43cf-b9bd-4a2562344e06.parquet
                    8470 promotion-226aa472cc894da787dfb0682638f10b/metadata/4e173977-b4d7-43f9-bfc6-ee7f980becda-m0.avro
                    8524 customer-a274f74636104d6384c3bffa84100cbc/metadata/28e7daa7-9145-4bc9-99f8-f22c1bbefd16-m0.avro
                    860213475 inventory-484b80a5590e449199b01b4ca507ab91/data/20260605_130833_00022_st26k-9c8e90a0-d1fc-42e4-bc7c-b2116a560ec5.parquet
                    874936 web_sales-28c8ab917b0a49faa78a5bfde99e515b/metadata/20260605_131953_00046_st26k-42216e8b-1d80-4ec8-a630-efa91c3daa72.stats
                    8787 item-153ad53ffd8948a4ad24ec5020485e4d/metadata/1015534d-3828-464a-9d10-d69f4b797be1-m0.avro
                    890530 catalog_sales-4db5f6164de44048a84ba10c7f17589d/metadata/20260605_130226_00008_st26k-d757af31-74c4-4605-ac2c-2df122152be3.stats
                    8942 web_sales-28c8ab917b0a49faa78a5bfde99e515b/metadata/00000-67501e29-033e-4127-bf1c-a1d43d86d3b6.metadata.json
                    8972 catalog_sales-4db5f6164de44048a84ba10c7f17589d/metadata/00000-38919732-7ec5-4b63-9aa4-9084fb80e827.metadata.json
                    9063 date_dim-66a709699883471684cd954c981e4b59/metadata/f38d7481-2ba4-4106-ac98-8764a1bf0cdd-m0.avro
                    9098 store_returns-9f1669793a40476bba696a31a5da1417/metadata/fee585f8-0def-40af-986f-9d9842ae6334-m0.avro
                    9117 web_returns-a227bf7382cb46f0827f84d7c4f65b29/metadata/99df06ec-9708-41af-874a-52aad4ce02e3-m0.avro
                    9129 web_site-c3dcd5b06e974e2b9b7a7182afd00678/metadata/611d5d70-b023-4136-98c4-c585b1ee9174-m0.avro
                    9342 store-1cbeb6c68e2b46f1a61f19fe8527b57a/metadata/75fa4b71-4280-4c1a-b971-0aed3bd71561-m0.avro
                    9388 catalog_returns-256bf8aeece34579bc0f9d19b2cb4543/metadata/8a781bc0-9f01-4e61-9ef6-f39a7e0ee41a-m0.avro
                    9517 call_center-d25f1f5f82b54eb4b952a5be304b7f73/metadata/c93d0c30-21a1-4b1c-9e07-5a3ae46973a0-m0.avro
                    9654 call_center-d25f1f5f82b54eb4b952a5be304b7f73/metadata/20260605_130145_00002_st26k-abe9d176-ae22-4a96-b98b-7ce5796bda51.stats
                    99304 time_dim-fb84514664e8431eb0884da596ed7a08/metadata/20260605_131934_00038_st26k-84dc84df-52f9-4f42-a053-ea44e84a0a08.stats
                    """;
            BenchmarkRunner.verifyDataListing(resolveTablesLocation(dataLocation), "Run `testing/benchmark-data/hydrate.sh iceberg-tpcds-sf100` first.", expected);
        }
    }
}
