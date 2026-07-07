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
                    10107 web_returns-7c46da8beb874de5898a39d9b68bd465/metadata/00001-669d3e02-db98-4591-832b-6dcd8246249c.metadata.json
                    10130 household_demographics-52b544018f094b5b8b925050feba174e/data/20230630_132808_00760_wcd7b-9d0bfb6b-cba0-483f-8d35-3a4a033c625d.parquet
                    103418 catalog_page-abf2b37a4b2d457ea75378295c39fa02/metadata/20230630_132741_00750_wcd7b-528e148b-2b91-4980-8c60-0146c3262cf9.stats
                    10501 web_site-3701731cfd7045c2bfad58de0a6f1450/metadata/00001-282c032e-0e60-48b2-b878-cc77b5144049.metadata.json
                    11063 date_dim-23d031cec1644a1c8123d56c8a4eafbd/metadata/00001-d676296a-28e3-48d2-a472-1addc2329029.metadata.json
                    11070 catalog_returns-b374fe5dc5b4463693183f386c251474/metadata/00001-76d347d0-57e5-4794-9783-7f3a8a3d7cfb.metadata.json
                    11318352 item-06036328e0fc4ea9af861e8eccb8a4d0/data/20230630_132812_00762_wcd7b-add29df5-678c-465c-807c-de136a98db85.parquet
                    11376 store-347a9a4c5a194e3d97066fb362f9d087/metadata/00001-30f4fc2a-b434-42c5-8a7f-6fd3ec800294.metadata.json
                    116435408 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-313fc066-5bf9-443a-a60c-4ac3d24cc19e.parquet
                    11663 call_center-cace8ff8957e41659bd77f8deae79ed6/metadata/00001-796c7725-4038-41a0-8c49-9c9f2ac82357.metadata.json
                    118192423 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-6be9e750-e2f0-48d0-b2c6-23bc9b6c770e.parquet
                    1195 reason-9a14387bef2b4d028b1d1dd987a9328e/data/20230630_132840_00766_wcd7b-c364db8a-3f5b-4a9c-aa23-e242b29095a8.parquet
                    119763516 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-310d37d7-c0c8-46b9-8397-2575e897f90a.parquet
                    1202 income_band-5b08ee2498c54dd1a19d4a089893d912/metadata/20230630_134826_00784_wcd7b-6dd24cee-32cb-4c01-bea8-c9c2d5c7e511.stats
                    121769448 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-4949a5cf-c1ea-49fb-abae-eabcc6f840c5.parquet
                    121880894 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-3058aac0-259a-435e-b25b-4c91cfa2f10e.parquet
                    1219583 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-0cfaf23a-758a-449a-92db-1075bf10ac2e.parquet
                    1220518 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-770a1ded-fe81-4669-9427-797ceebc38bc.parquet
                    1221203 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-a73e0310-751d-47d1-84c4-62cae04a8e58.parquet
                    1221222 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-72b5b8cd-7fc2-4fe1-bc68-3a54595ee3ab.parquet
                    1221772 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-74036388-edd7-44e8-851f-c328a8250f13.parquet
                    1221800 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-29f7da11-09c4-4cdf-ae59-15de16b83b8a.parquet
                    1222772 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-0a0ef944-b0a3-463a-9880-fa8d30c8a3d0.parquet
                    1224080 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-312e6ef0-1960-459f-a15e-9ba9b26e0ccd.parquet
                    1227205 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-828e601a-7d27-465a-a7fc-7958e4ff12a0.parquet
                    1228270 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/data/20230630_132753_00754_wcd7b-eaddd363-034d-4053-9db1-a399b7459ec5.parquet
                    12356 catalog_returns-b374fe5dc5b4463693183f386c251474/metadata/b91874e5-b45a-452d-99a3-de50a6fcb3f1-m0.avro
                    124801119 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-e9aa345f-d73c-4f6c-8244-7c9ce05cea1c.parquet
                    1254170 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-e1ec8e5d-2e68-44d4-b61b-06ba38c71372.parquet
                    126964432 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-951fb0c8-8475-4b40-9ea2-81e2694fb2dc.parquet
                    127770486 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-1a4def72-ef17-42ba-bad6-51589bb44884.parquet
                    127856443 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-c1fe1a33-0fbf-4181-9bf6-7d721dfc5134.parquet
                    128287217 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-74251e13-249a-42ea-83e7-48cba3cac4da.parquet
                    130889748 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-068b77a8-a9af-4ada-a44d-41d798a961d0.parquet
                    13318 web_sales-9485b1bc457a475299e2e43e01c5fa76/metadata/00001-f0b5e89f-971f-45d7-a65d-252fbaaa396a.metadata.json
                    13348 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/metadata/00001-db7b1971-d4d3-4a36-bffa-adde803f1eef.metadata.json
                    133664545 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-fac9d420-299b-4796-ac0e-b2b8880d5efa.parquet
                    134310099 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-beae96c8-bca4-4d5f-88ec-15c92b23f3ff.parquet
                    135434241 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-af481d99-4920-497f-9fb1-027593c8087f.parquet
                    136259644 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-88fc6b64-b2f2-4ba7-8613-a99eee20c298.parquet
                    137084406 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-83b64177-4d07-4842-9e77-52316c1cc4e1.parquet
                    138046167 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-28888694-b6f8-4bfd-bc47-2ce765897924.parquet
                    139491712 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-11bc8d2a-db96-4738-9b55-f50dac031ac5.parquet
                    139536128 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-03ed9764-77d0-443a-9f76-4bf4ccdc07a0.parquet
                    139993055 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-db62861c-e9fb-41e6-b81b-b5ef30b80a36.parquet
                    14030 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/metadata/5c49fa49-1458-4cb9-a892-02e6f7e0a891-m0.avro
                    142028669 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-94c3e706-5154-413d-9cba-2a2cff78bc59.parquet
                    148940716 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-ffebc88f-40f8-441d-a618-4e711c8f9094.parquet
                    148950468 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-130c0b43-5957-4743-b4dd-96de30605d8c.parquet
                    150398079 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-19f9d3a4-5c08-46cd-aae6-8d4c743e13a5.parquet
                    151183561 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-46beb2db-1ebf-493f-a8a1-9e4ce1f005e1.parquet
                    15410 web_returns-7c46da8beb874de5898a39d9b68bd465/metadata/aad3b3ca-50dc-4fa0-a41e-b31fbf335bbe-m0.avro
                    156053401 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-4021bdfa-3e89-4cbc-a18f-fc0a28340a35.parquet
                    156282147 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-46bf6423-be20-4c37-b880-5c71ba32bd54.parquet
                    156823580 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-370b5986-5c90-4eba-b188-3f0f5b777fb7.parquet
                    157819020 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-3935910e-5345-4ad5-885a-164cfd6cc431.parquet
                    157887947 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-dc5953c3-6265-49f2-ae78-d09cf76e9460.parquet
                    1582844 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-4b23ab19-a926-4665-a536-4e280ee2fafd.parquet
                    158318134 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-edca519a-fede-4cf5-aa47-bca96e979e93.parquet
                    158411660 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-b6edde8b-7896-4f6b-bdaf-71eb8d8d72db.parquet
                    159392790 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-80bf3974-e38e-431b-80a8-bf8ab948c676.parquet
                    159803409 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-37eed663-f144-4d62-9322-75c3510df770.parquet
                    160062262 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-2ffe21e4-a333-4b87-b3a9-df8e17fb00c1.parquet
                    160468154 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-a16717de-663e-4abe-b1a6-fcb4ede129d0.parquet
                    160693597 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-71f9f2ba-b943-4290-8f7e-f5cda8a7798d.parquet
                    162123 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/metadata/20230630_132753_00754_wcd7b-7923f03d-1f0b-4d96-a03d-5c3c70186438.stats
                    162618977 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-5265bc34-298d-4987-8033-849110a560c5.parquet
                    163624426 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-4b1aa021-6fb5-4164-ae65-761d9c12bf5f.parquet
                    166846924 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-623fb6e3-3f0a-4894-9fc2-a5714fd0162b.parquet
                    1695 ship_mode-779915a8bd2146d39eb94d67ebcc9af8/data/20230630_132844_00768_wcd7b-177dc618-bcf8-4c5d-9bf4-e80d1d22ff49.parquet
                    170018 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-32dbe6f9-0c0a-47f5-b66c-15f4f195bae8.parquet
                    182540403 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-4e6c40c1-d8f6-4d93-80fb-e4c3177d4efb.parquet
                    1987969 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-959089a9-53b1-4d06-a92f-b2bdde1aee8c.parquet
                    199010045 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-3a47b3b1-bb94-4c83-b077-f225dee6b94b.parquet
                    200323121 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-b1d41cf4-a860-4097-9d86-5a837738eeaa.parquet
                    200991 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-39ac068a-9228-4383-9093-568b3269ccbd.parquet
                    201002 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-3ca8e7c8-ebaa-479a-80bd-3f7c7d190ef3.parquet
                    201072 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-f93bd1f0-38c6-4f47-a3cc-5858351e9441.parquet
                    201150 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-475a7787-1f7f-4b0b-9a8b-60daf2c3c4b9.parquet
                    201189 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-00c7b642-2af3-4349-a694-cb716c2a28af.parquet
                    201263 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-f5ebbe27-4dcf-4c23-becc-569d713f08c2.parquet
                    201372 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-64134d1a-faaf-4538-bf4c-e57c2680dac4.parquet
                    202651 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-91b49cdc-bc87-449c-9e52-4d7a0fc3d562.parquet
                    2037 reason-9a14387bef2b4d028b1d1dd987a9328e/metadata/20230630_132840_00766_wcd7b-f087a718-f85f-4bd7-837a-6c2a621f9af8.stats
                    2053467 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-f9674e73-4c6a-4383-b0d9-19259ad7f6f9.parquet
                    206407 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-627cc1c6-bd0f-488c-8188-a88ba5cc698f.parquet
                    2083 ship_mode-779915a8bd2146d39eb94d67ebcc9af8/metadata/20230630_132844_00768_wcd7b-2dac3e78-693c-471b-b7d8-934eb830b2fb.stats
                    208830 customer_demographics-b155cd14884349a4a15b299f4e6cad95/data/20230630_132758_00756_wcd7b-17e2b105-6f10-4167-bd30-261065d29e4f.parquet
                    208968 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-2cfba9e9-e331-4c46-ab6e-a9897fbca3f6.parquet
                    209533644 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-011a4d23-50e1-42eb-bb0f-a46c0de357d2.parquet
                    209595739 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-f149583e-c001-4a7d-b5d6-26ae9eee0b2d.parquet
                    210182421 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-f3736d1d-59a8-4113-a248-3b4c2293f278.parquet
                    2118 reason-9a14387bef2b4d028b1d1dd987a9328e/metadata/00000-232f86b0-6715-4fc6-ab63-4539296c8fe9.metadata.json
                    212974746 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-3cb76bd3-d5c7-485e-b186-94c5d135b8af.parquet
                    2130 income_band-5b08ee2498c54dd1a19d4a089893d912/metadata/00000-8af64f5e-d843-442e-9f04-48d94164ae12.metadata.json
                    215850016 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-e6563ed1-de37-4716-96fe-a3196c13b3ea.parquet
                    222983711 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-544fc0b8-d9b2-4fd9-9e3f-b2da59dd50c2.parquet
                    225938653 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-fde74c12-eea6-4d8d-9ab8-7390f3a16717.parquet
                    2260 inventory-04a35aa569bd439cad7cb616a9abde08/metadata/00000-e26906d7-4999-4df0-87c9-ed3cf8efe854.metadata.json
                    2372 household_demographics-52b544018f094b5b8b925050feba174e/metadata/00000-ee7e9283-8154-416b-b6bc-416b76ce54a6.metadata.json
                    24239 web_page-03c886c64249498e97a74f62404be20d/data/20230630_132900_00776_wcd7b-99b94491-9c5e-4cf1-8730-b58f17649a5d.parquet
                    2436 ship_mode-779915a8bd2146d39eb94d67ebcc9af8/metadata/00000-38e98a82-0a74-4c91-99b7-e9ad9ea6bc0f.metadata.json
                    24364300 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-61d55fb6-ab6c-4394-ab64-4231e6b257bd.parquet
                    24429512 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-46e3a751-1b7a-460f-a0df-548aba0497ba.parquet
                    2519758 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-6a03cefb-5132-4f66-b51b-3a8aafd3362a.parquet
                    25557962 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-bef08410-dc51-470e-875b-5d2b38e0c02b.parquet
                    25572993 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-76ff1bd4-a079-4619-ae97-0c37689b9512.parquet
                    25591250 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-5e4699fc-591d-4931-a6de-e24217480f32.parquet
                    2564409 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-3749fca0-813f-43b4-a171-8366603b2be7.parquet
                    2571172 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-19d7cd91-dd55-42b4-9204-c31882311712.parquet
                    25822285 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-61884418-af2b-4118-8cbe-9c15e4a822a0.parquet
                    26011126 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-1fea84df-6646-4249-ac09-6a025ac35c40.parquet
                    261000 item-06036328e0fc4ea9af861e8eccb8a4d0/metadata/20230630_132812_00762_wcd7b-633b425a-adb3-499b-b832-9b1ce8a83d0b.stats
                    26185340 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-a4787f28-6346-42e8-8e95-f21be80708f1.parquet
                    26234326 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-d9a56cb0-413e-47b3-b85e-7bce61c9fe3d.parquet
                    26345380 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-2c29225e-b5de-41df-933e-10aa0ed59c59.parquet
                    26361194 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-2e72ae42-72b3-4975-af0c-2fc10ef6e4a8.parquet
                    26369672 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-bda808c7-7f2a-4a32-93e0-6c21c6931b4f.parquet
                    26536120 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-fa7decc2-2ae5-4ee1-a3a0-d4031d3c2de9.parquet
                    27381094 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-ca8512c8-8257-425e-b25c-4bfb9845e604.parquet
                    275154769 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-779e8f79-fa1e-40db-a9d2-c2b52eeea0db.parquet
                    27707 web_sales-9485b1bc457a475299e2e43e01c5fa76/metadata/47cf3e96-bcb4-463e-9a5e-a8f794fdb006-m0.avro
                    277152465 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-57f67fe9-13b9-4144-8342-dba3d99a91d0.parquet
                    2796 catalog_page-abf2b37a4b2d457ea75378295c39fa02/metadata/00000-06cb75a2-89e0-47b0-b8fd-3d065574b363.metadata.json
                    28045856 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-bb200a25-2cbc-4945-8939-6029b972600b.parquet
                    2827 customer_demographics-b155cd14884349a4a15b299f4e6cad95/metadata/00000-87da5d21-a196-4afb-84bf-9d6ca69035d1.metadata.json
                    2827 time_dim-1a224b45329540558d44b3790118b562/metadata/00000-bc595f65-4952-4370-8434-458b17933151.metadata.json
                    282928242 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-06d64db3-524c-42c0-b4f2-57fc6ba4c6c1.parquet
                    283913511 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-593f4768-7330-4e6e-98bb-351e7d68380c.parquet
                    28445 store-347a9a4c5a194e3d97066fb362f9d087/metadata/20230630_132848_00770_wcd7b-740e84f0-b7f0-4c32-8184-c66708ebdf47.stats
                    284840542 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-02aab519-799c-443f-8d98-a9a47066dfe6.parquet
                    285140082 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-529a1090-7eb1-494f-8dc0-f4903f640eb0.parquet
                    28707 store_sales-dbf6ed25869c4fb08651af77f50a9a07/metadata/d21210fa-5ff4-4930-8a2d-e0a451e64a5a-m0.avro
                    288538031 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-e9cc328a-67b3-47ac-a145-f3c8f7bd526b.parquet
                    2885477 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-e462f682-a71e-4884-b686-3b85451e703c.parquet
                    288591918 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-a2e5b5bc-3f9b-4ce1-814d-66bc69ee64e8.parquet
                    28898912 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-fb1382f3-7c36-49e2-8884-54dcb93c24cb.parquet
                    289766999 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-6e8d6e5a-f64d-420c-9bb7-0f83405a10ad.parquet
                    2910029 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-08680573-c27d-4026-924b-436fbcdd6c22.parquet
                    298080618 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-cd937ac4-3368-4907-b8c3-d994e187a2a1.parquet
                    29927968 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-d283179a-8b7a-4aa1-ade3-e702154a522a.parquet
                    3016145 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-c5b2cee4-d04c-42fa-acf4-89ffdf72e16a.parquet
                    30373 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/metadata/c82a4ec2-499c-426c-a406-708aa390c2e6-m0.avro
                    30523 store-347a9a4c5a194e3d97066fb362f9d087/data/20230630_132848_00770_wcd7b-939b4b9c-585e-4625-afb6-5a04a16770e8.parquet
                    305493902 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-214e1f84-5a88-4ed8-a1ab-5ccd32e1aadc.parquet
                    307336336 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-fffd92e0-2ff6-42a8-a1de-629a70f6788f.parquet
                    309904947 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-50c1c122-1465-4cde-90e3-9310d9702d74.parquet
                    311939114 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-b0dbc1c9-935e-45ab-aebd-2c450d64ec01.parquet
                    312132 date_dim-23d031cec1644a1c8123d56c8a4eafbd/metadata/20230630_132803_00758_wcd7b-54fdc7f0-295b-4ff5-9893-8a8f843d714d.stats
                    312641544 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-c7da5835-c266-4115-988a-7395bf86cd3f.parquet
                    312737993 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-7c471b24-cbd1-4e78-947f-0a4aaf603d39.parquet
                    315972581 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-80b3f995-c556-4ad4-9a0d-acb28e69513f.parquet
                    31615804 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-0b995ebf-5890-4f15-b0c6-a9dd91ecb6d0.parquet
                    316216444 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-68183cef-381e-45b1-b1f8-93d5f23d29e0.parquet
                    316388403 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-fa33a340-4ee0-4d50-bb42-49fb7aaa0d29.parquet
                    320082 customer-3f927ff1d32d4935aed0180e48f74019/metadata/20230630_132746_00752_wcd7b-35759371-f8b8-4e77-9cd5-73973d1e40f1.stats
                    3226389 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-83af1db0-7231-4501-9465-c55b45451739.parquet
                    3229 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/metadata/00000-e6f1cafc-7f95-441c-9652-6e46fdf7fb4b.metadata.json
                    326783063 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-7ade8d14-03fe-4546-bc5c-aea8ddfc1ac1.parquet
                    3296 warehouse-900596e26c504434b5d77a5c599a70a2/metadata/00000-636c458f-1f7f-4679-a178-2493b4829696.metadata.json
                    3304 web_page-03c886c64249498e97a74f62404be20d/metadata/00000-393a2c35-3312-4243-8463-4f4d8ef111a7.metadata.json
                    3311 reason-9a14387bef2b4d028b1d1dd987a9328e/metadata/00001-61eff168-f3d5-4e38-85fb-85ec98195e67.metadata.json
                    33193257 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-4abadb85-4fd8-489d-995c-4b56de89306e.parquet
                    33316 customer_demographics-b155cd14884349a4a15b299f4e6cad95/metadata/20230630_132758_00756_wcd7b-18f5e7aa-611a-45ec-bf6d-04a7cbea0c33.stats
                    3333 income_band-5b08ee2498c54dd1a19d4a089893d912/metadata/00001-c287ed02-902b-4153-ba47-9ce2acd9ac68.metadata.json
                    33391 promotion-1c49844df9ae466080df3a48d07e30fe/data/20230630_132836_00764_wcd7b-3a51129a-7637-45f4-8cf4-aa38c69d6b98.parquet
                    34273 household_demographics-52b544018f094b5b8b925050feba174e/metadata/20230630_132808_00760_wcd7b-598ea4ea-37bd-40c8-9b40-afac444c3b01.stats
                    3439 warehouse-900596e26c504434b5d77a5c599a70a2/data/20230630_132856_00774_wcd7b-32fdcbfb-96bc-4928-8eb7-99bbcd29a83c.parquet
                    34440792 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-03166182-61e2-44cc-ab48-7ee06562168d.parquet
                    35217583 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-79076db7-ca64-430f-b645-e17182eaf7fe.parquet
                    35302565 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-30df6608-98d1-47d9-b6cf-f0088144e89f.parquet
                    35316217 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-da9e7b47-19b7-4a2d-ab2e-af387958ab29.parquet
                    35396604 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-9eaaf897-015a-4007-b221-0cdacbfd6375.parquet
                    35584149 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-a3af3141-4cf8-48db-87d5-c7d34c59f048.parquet
                    36142784 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-ad45cd75-585c-41b5-94ab-cdc716b1e714.parquet
                    36156036 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-b8a9dcd7-dfb8-4092-9a27-c017bb2aae4f.parquet
                    36470096 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-0d97988b-3210-4e25-9fbc-210260ec1883.parquet
                    36479786 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-b832522e-710c-4ca8-bc70-e0574032b7b8.parquet
                    365763 time_dim-1a224b45329540558d44b3790118b562/data/20230630_132851_00772_wcd7b-e0be3dc9-fc36-4735-bfdd-0f8766c151fe.parquet
                    3671 inventory-04a35aa569bd439cad7cb616a9abde08/metadata/00001-4b8cd23c-168a-4db5-8f18-ef615f216e7b.metadata.json
                    37400997 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-4d4d9e4d-fd1f-47cb-9135-6e50307dee14.parquet
                    3778 customer-3f927ff1d32d4935aed0180e48f74019/metadata/00000-159931b2-09d5-49c9-8e66-c20bc2745455.metadata.json
                    3861 promotion-1c49844df9ae466080df3a48d07e30fe/metadata/00000-1c823e9c-72c1-4dde-b897-263441c77068.metadata.json
                    393027 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-7cca643e-0419-4a48-8625-c60b26611a21.parquet
                    39909702 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-9f77210b-f3f5-49f8-a785-c72f62a47ac4.parquet
                    4007 household_demographics-52b544018f094b5b8b925050feba174e/metadata/00001-ff8967f2-56af-4a57-b80b-2bdd94db064f.metadata.json
                    4056 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/metadata/00000-9c3ecf32-305b-4114-bb32-1bf540e35584.metadata.json
                    4112287 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-25587199-6489-4b24-86c6-6d39b0604a60.parquet
                    4130 item-06036328e0fc4ea9af861e8eccb8a4d0/metadata/00000-3e1442e1-82cb-49e6-a9e0-3b333776df06.metadata.json
                    4246 ship_mode-779915a8bd2146d39eb94d67ebcc9af8/metadata/00001-cf2ce7a3-1939-4cad-a7e3-194b625c8662.metadata.json
                    42460437 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-0e8efdb9-29ff-473b-9790-9b08ec8f25c5.parquet
                    42791 inventory-04a35aa569bd439cad7cb616a9abde08/metadata/20230630_134829_00786_wcd7b-d92527b3-bc58-4ff9-909e-ff026a6c9bb4.stats
                    4297 reason-9a14387bef2b4d028b1d1dd987a9328e/metadata/snap-3909257532506295957-1-a9969df9-fb38-4c54-982e-9e8973d02726.avro
                    4300 call_center-cace8ff8957e41659bd77f8deae79ed6/metadata/snap-23777363804513287-1-5a9cf442-82d0-474a-aadd-c5eed1e3065b.avro
                    4301 warehouse-900596e26c504434b5d77a5c599a70a2/metadata/snap-1629885389373522431-1-c05f3667-a92a-4407-b037-6447c9dbee81.avro
                    4304 income_band-5b08ee2498c54dd1a19d4a089893d912/metadata/snap-4380650175380202431-1-29af9e3b-7a7c-4bf1-ac72-70dd56055148.avro
                    4304 ship_mode-779915a8bd2146d39eb94d67ebcc9af8/metadata/snap-8981849558860475431-1-2858fa88-d110-49c8-bcc9-3a2dfe92b94e.avro
                    4304 store-347a9a4c5a194e3d97066fb362f9d087/metadata/snap-378381103188078755-1-e218e600-cffc-4ab1-b6fe-d3ccb8a99985.avro
                    4306 web_site-3701731cfd7045c2bfad58de0a6f1450/metadata/snap-5403995444599905666-1-7f4869cf-2d18-41a1-a2fb-68f742704877.avro
                    4307 item-06036328e0fc4ea9af861e8eccb8a4d0/metadata/snap-8179111685339113812-1-82e9d78d-9822-4337-9a3e-7cc80ef118ca.avro
                    4307 web_page-03c886c64249498e97a74f62404be20d/metadata/snap-1959567675026569409-1-0d55a6c7-b15e-4643-8e70-74b5909d471d.avro
                    4308 time_dim-1a224b45329540558d44b3790118b562/metadata/snap-8454883159942881848-1-269929ef-f2d2-4ff2-82ac-63d6b3e582b3.avro
                    4308 web_returns-7c46da8beb874de5898a39d9b68bd465/metadata/snap-189911219465445288-1-aad3b3ca-50dc-4fa0-a41e-b31fbf335bbe.avro
                    4309 customer-3f927ff1d32d4935aed0180e48f74019/metadata/snap-3372709042683971658-1-9add9a6f-22fe-4c3e-864a-6852eb81cad0.avro
                    4309 promotion-1c49844df9ae466080df3a48d07e30fe/metadata/snap-6148134406291740947-1-72740738-7ff8-4aa3-8c63-20211d71f5dc.avro
                    4310 catalog_page-abf2b37a4b2d457ea75378295c39fa02/metadata/snap-7257223470418027845-1-5d61698f-c759-4be8-a5c6-a47998328e93.avro
                    4310 date_dim-23d031cec1644a1c8123d56c8a4eafbd/metadata/snap-2717772410240956368-1-7a486ff9-1976-49a0-989c-ff0c2c85ccb6.avro
                    4313 inventory-04a35aa569bd439cad7cb616a9abde08/metadata/snap-6487023564888034706-1-cafd335c-3297-4969-8534-53c7f72d051b.avro
                    4313 store_sales-dbf6ed25869c4fb08651af77f50a9a07/metadata/snap-8992618808318211724-1-d21210fa-5ff4-4930-8a2d-e0a451e64a5a.avro
                    4314 warehouse-900596e26c504434b5d77a5c599a70a2/metadata/20230630_132856_00774_wcd7b-c6450a06-dc1b-4164-b0bb-2405892a81d6.stats
                    4315 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/metadata/snap-1780799882562445570-1-c82a4ec2-499c-426c-a406-708aa390c2e6.avro
                    4315 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/metadata/snap-5306494208623092420-1-5c49fa49-1458-4cb9-a892-02e6f7e0a891.avro
                    4316 catalog_returns-b374fe5dc5b4463693183f386c251474/metadata/snap-516205123334673863-1-b91874e5-b45a-452d-99a3-de50a6fcb3f1.avro
                    4317 household_demographics-52b544018f094b5b8b925050feba174e/metadata/snap-6115721203956582408-1-51b04934-93d8-482d-bcaf-d41de5f989d9.avro
                    4317 web_sales-9485b1bc457a475299e2e43e01c5fa76/metadata/snap-8633303587285108635-1-47cf3e96-bcb4-463e-9a5e-a8f794fdb006.avro
                    4319 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/metadata/snap-5742632154976568204-1-30dddc44-91c4-4e49-88a1-d2e6877bf771.avro
                    4325 customer_demographics-b155cd14884349a4a15b299f4e6cad95/metadata/snap-7582567643331509935-1-566a0227-c06d-4c04-8f02-3cfe62a5d676.avro
                    4399 store_sales-dbf6ed25869c4fb08651af77f50a9a07/metadata/00000-afa8961c-48d0-450b-8cac-709cec38ce79.metadata.json
                    44137417 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-88a3ed89-0f45-4ad3-b752-74c4b2a67d10.parquet
                    44177746 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-15b8eb6a-b3e7-4e22-b381-b2ffa5b868e0.parquet
                    44213311 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-2aec07fc-3c0e-4bfb-8016-acd17decc6c6.parquet
                    44390534 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-1be77aea-f8f7-460c-b836-54bc7eeb1b80.parquet
                    44574 web_page-03c886c64249498e97a74f62404be20d/metadata/20230630_132900_00776_wcd7b-88e99c46-7a61-446c-963c-46c35bb63aa1.stats
                    44656295 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-b869296b-9919-403d-b9a2-1b1f6a39d6b2.parquet
                    44892639 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-a76a5da3-4fd3-4f27-9c50-e18386440b15.parquet
                    45095189 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-1cb29527-fe90-4430-b1c9-1e53b2d9b691.parquet
                    45163 promotion-1c49844df9ae466080df3a48d07e30fe/metadata/20230630_132836_00764_wcd7b-f092ed2e-4fd8-4092-aa6b-82cbe54daf0b.stats
                    4543 web_returns-7c46da8beb874de5898a39d9b68bd465/metadata/00000-f1893d5b-7792-4bfa-856f-2534d309b6d7.metadata.json
                    45652568 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-904f909d-103e-442b-8c71-7c1d9525c5dd.parquet
                    45802523 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-f04094c5-e487-45ed-bca1-24174e76cab0.parquet
                    45883809 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-71e41295-819d-4bc7-8e04-e83d099b11f3.parquet
                    4607 web_site-3701731cfd7045c2bfad58de0a6f1450/metadata/00000-a0c3a9d2-a25c-4b79-81c9-5991a7090f0b.metadata.json
                    4723 date_dim-23d031cec1644a1c8123d56c8a4eafbd/metadata/00000-8587d8a6-89f1-4e0c-9443-22fbf879f57e.metadata.json
                    4884 catalog_returns-b374fe5dc5b4463693183f386c251474/metadata/00000-7904fb73-4e51-41eb-991c-d20db79c9982.metadata.json
                    4892 store-347a9a4c5a194e3d97066fb362f9d087/metadata/00000-263d55e9-d8a9-4c07-af8c-4360a25dffb9.metadata.json
                    49956417 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-5a160822-f23d-42d5-b0db-d1ed01087bd1.parquet
                    499915328 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-506e469c-8d7c-48af-92fd-a22f9781e50f.parquet
                    5006 call_center-cace8ff8957e41659bd77f8deae79ed6/metadata/00000-fe0b68c3-7218-47d2-9039-6a6be55c8a52.metadata.json
                    501041540 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-edcd8f28-f114-4b89-ae83-a4a8caf6d2fb.parquet
                    501623682 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-264e6fb2-9299-4997-85d1-aaf78021434c.parquet
                    5073979 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-e54bd15f-a2ce-4c50-b0d0-8fa55820f331.parquet
                    510319727 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-1a4e117d-f99c-494a-b3b7-7c87ea8663cc.parquet
                    515237996 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-c48c6460-681d-4c4a-a098-c32bfbd95af2.parquet
                    516998157 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-d4a683a8-f620-4477-a9e6-d8c008289817.parquet
                    5184737 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-f43791ec-e963-46ab-9306-776ce93e52c1.parquet
                    5193913 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-0fde2ecc-9854-43f2-b0bc-4670a2d17a42.parquet
                    519432309 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-3cc1d0e9-f1e2-479b-81aa-88f180ee9567.parquet
                    519615943 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-88cde56d-2730-4bc0-a8aa-0c63cf17de7b.parquet
                    52075261 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-97ae9897-27ed-4cbd-a451-60b575593c52.parquet
                    5237 catalog_page-abf2b37a4b2d457ea75378295c39fa02/metadata/00001-710d87f8-6a17-4000-92ba-72ec70bb4394.metadata.json
                    524034 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/metadata/20230630_134903_00788_wcd7b-5b651a25-eb1a-4cef-9cf2-34cdefd5bafa.stats
                    5251148 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-10b4ac1c-ba27-4422-806c-f5820ef6519d.parquet
                    5274 customer_demographics-b155cd14884349a4a15b299f4e6cad95/metadata/00001-8f910730-fd8b-4e13-b7cc-044f35fe1b57.metadata.json
                    527523071 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-e9c762d1-4504-41cd-87a6-6e34b63ed53e.parquet
                    527781776 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-5b0efa1c-8d3e-4e1d-9200-4f6e768aa6cc.parquet
                    53539367 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-72c1ef92-7e87-4c9b-baf6-71ba6e423188.parquet
                    53910724 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-72f21243-e179-4661-b911-379554f486da.parquet
                    54531827 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-dc428860-dff4-481e-ac04-2ed551f548ff.parquet
                    5460 time_dim-1a224b45329540558d44b3790118b562/metadata/00001-d6cc68bd-1acf-42ab-9dd6-969bfc69c080.metadata.json
                    5533349 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-5e9b2a6c-b936-41d3-b53a-2ced19954b2e.parquet
                    557254 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-b7f4d9f6-5dd4-4bdf-a5e5-2634813b4e1f.parquet
                    55783919 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-c7fbc701-19b7-43b4-b276-96335359d6c8.parquet
                    56078432 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-35154759-e274-4331-a1fe-623eb6fa1699.parquet
                    5650 web_sales-9485b1bc457a475299e2e43e01c5fa76/metadata/00000-67c54797-b9d2-4874-bb63-890c5e39add8.metadata.json
                    5650102 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-2dc8d750-1aaf-4def-88c6-c39d6c6d4d76.parquet
                    5667 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/metadata/00000-9ef50335-0165-4f46-b7a3-f1b3025524f9.metadata.json
                    56674965 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-d3d8dfe2-d254-47a7-965c-3369a89b1e4c.parquet
                    5764023 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-f02fd053-4649-4919-8b7f-2e5e8df171c5.parquet
                    5784039 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-2e3b4f59-38d5-48cf-b610-3d148939ea3d.parquet
                    59489830 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-9c532959-3fac-4208-a87a-da1a838876bc.parquet
                    60159914 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-f2aa147a-aa9f-4784-bd80-b9b8c6561c5f.parquet
                    60501365 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-2b096d86-40aa-4760-a637-754e64a378d0.parquet
                    60592040 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-792a4e75-9706-41ef-bd06-31eb11360abf.parquet
                    60801124 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-d27ad1e9-d761-461f-b874-bc972c5bb1e3.parquet
                    60940063 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-7c303abf-a721-4b4f-88f9-7caa91464ecc.parquet
                    61089557 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-f2c673b2-17a7-433f-be18-93f5c5592bee.parquet
                    61177565 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-c96f71e3-b627-4981-9cb8-e37d103561be.parquet
                    61227275 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-b459dce0-0dd1-4282-b0b4-aaf70bfc0d09.parquet
                    612317 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-0727157d-0151-4685-9b52-9becae5d4723.parquet
                    61273470 inventory-04a35aa569bd439cad7cb616a9abde08/data/20230630_134829_00786_wcd7b-c874d10c-c88f-479f-85a1-90ed6e1504a0.parquet
                    6135732 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-aecadf64-5512-4848-be15-019f358a44be.parquet
                    62210953 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-e1d9bb27-8341-4f1c-a2e6-9ec2794c323c.parquet
                    624785 store_sales-dbf6ed25869c4fb08651af77f50a9a07/metadata/20230630_134929_00790_wcd7b-a02475fc-bf32-4138-bf5f-aba78e8e1148.stats
                    625774 catalog_page-abf2b37a4b2d457ea75378295c39fa02/data/20230630_132741_00750_wcd7b-9cc7e64d-5fe7-4f06-992e-a36262f6d232.parquet
                    6303278 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-8e8c0fea-f92f-4252-8851-748ed5249fdf.parquet
                    6386585 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-cac2ac6e-236d-4af4-818b-ae5f95d891e9.parquet
                    6434335 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-905de91e-31dd-44e7-81e3-07cae2bf21a6.parquet
                    6505 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/metadata/00001-dceaef36-8d98-4f8c-9a9b-04e2cf8dda63.metadata.json
                    6516894 web_returns-7c46da8beb874de5898a39d9b68bd465/data/20230630_135448_00795_wcd7b-64925731-337a-4a2e-a77e-c54e06ee2458.parquet
                    666754 web_returns-7c46da8beb874de5898a39d9b68bd465/metadata/20230630_135448_00795_wcd7b-1ac25cd0-da1b-402c-aed3-519a12b4bf68.stats
                    6741 warehouse-900596e26c504434b5d77a5c599a70a2/metadata/00001-9a2565f5-6729-44ac-b55f-6f39c68cf145.metadata.json
                    6754 web_page-03c886c64249498e97a74f62404be20d/metadata/00001-fb36b9e8-30c9-44a6-b202-b58171ad63b8.metadata.json
                    682105 catalog_returns-b374fe5dc5b4463693183f386c251474/metadata/20230630_132908_00780_wcd7b-7fcc7041-e47e-42c4-8ceb-adbfd163759e.stats
                    6843 income_band-5b08ee2498c54dd1a19d4a089893d912/metadata/29af9e3b-7a7c-4bf1-ac72-70dd56055148-m0.avro
                    6852 reason-9a14387bef2b4d028b1d1dd987a9328e/metadata/a9969df9-fb38-4c54-982e-9e8973d02726-m0.avro
                    7018 household_demographics-52b544018f094b5b8b925050feba174e/metadata/51b04934-93d8-482d-bcaf-d41de5f989d9-m0.avro
                    7115 ship_mode-779915a8bd2146d39eb94d67ebcc9af8/metadata/2858fa88-d110-49c8-bcc9-3a2dfe92b94e-m0.avro
                    71380183 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-e638f3cf-d298-483c-b5a8-c0f3d8cbbeed.parquet
                    71491599 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-64b2864d-3a87-4858-abb0-b22d2d2fcba9.parquet
                    71665477 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-015ea2bd-02f1-42b7-aff1-1d1aeec57491.parquet
                    72939158 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-ab261967-1fca-4904-905a-90b928dd9823.parquet
                    7319981 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-e240b6c6-a30f-492e-9830-2a42b10c5ece.parquet
                    7321951 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-4ece2a2f-81d9-4a8a-9df9-487a67920611.parquet
                    7324099 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-878b286f-d915-4eae-bb31-310180ca97cc.parquet
                    7326028 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-7b96128a-2d5f-486c-93b1-84f0372a3361.parquet
                    7326332 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-d7f91da9-f652-474c-90a5-30dbb0f0dd1c.parquet
                    7326507 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-e197ba86-ebd6-4a9d-869c-b1448eba8837.parquet
                    7328916 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-e0fc71b2-4c95-4704-a60a-542b1a90302f.parquet
                    7331437 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-f008e7dc-f38b-4029-aafb-4ddf94a28cae.parquet
                    7334977 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-04a2fc97-187a-40ef-9d76-491e0b5fa1d4.parquet
                    7335648 customer-3f927ff1d32d4935aed0180e48f74019/data/20230630_132746_00752_wcd7b-0dd3875d-a7de-46a4-9a0d-a3cc8b6305e0.parquet
                    7366 time_dim-1a224b45329540558d44b3790118b562/metadata/269929ef-f2d2-4ff2-82ac-63d6b3e582b3-m0.avro
                    73980624 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-f0e61145-a71d-4d03-b987-0166bbf507a9.parquet
                    7408 catalog_page-abf2b37a4b2d457ea75378295c39fa02/metadata/5d61698f-c759-4be8-a5c6-a47998328e93-m0.avro
                    7418277 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-ad364d7c-906a-442f-ad9f-e384b85aecfb.parquet
                    744 income_band-5b08ee2498c54dd1a19d4a089893d912/data/20230630_134826_00784_wcd7b-762e9dc4-724f-4030-9e3c-855275aa5de2.parquet
                    74430836 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-ea36a64e-cfb2-430d-aae3-3ab25eb69e5f.parquet
                    74487234 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-fde1786f-8235-46f6-a460-25a6f9cf15b0.parquet
                    75546240 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-1344e658-cfdc-4280-81f1-07bb71a7c30b.parquet
                    75641189 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-243779a6-e0f9-4220-aa9c-b8383632684b.parquet
                    7694000 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-b01eff12-68cc-4317-a4d7-5246baac0f41.parquet
                    7762 web_page-03c886c64249498e97a74f62404be20d/metadata/0d55a6c7-b15e-4643-8e70-74b5909d471d-m0.avro
                    7770 warehouse-900596e26c504434b5d77a5c599a70a2/metadata/c05f3667-a92a-4407-b037-6447c9dbee81-m0.avro
                    7794 web_site-3701731cfd7045c2bfad58de0a6f1450/data/20230630_132904_00778_wcd7b-1127a893-18a7-445b-87e0-cf34a823ae3e.parquet
                    77994267 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-c84ca336-c6fc-451a-84ed-2afe3d2db0f2.parquet
                    78108480 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-2bba4217-e571-4481-a14e-bdfa12c5c122.parquet
                    78155019 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-01bc04fe-daad-4408-9b6a-d5f81b31629e.parquet
                    78647550 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-318e4233-08fe-41f8-8df0-4243bbbd0179.parquet
                    78816733 store_sales-dbf6ed25869c4fb08651af77f50a9a07/data/20230630_134929_00790_wcd7b-cec1ec0c-f2bf-4a0b-a77f-092dce4e0aa5.parquet
                    78838930 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-858f39ed-439a-435b-ad8b-c211e9dc6fe1.parquet
                    78850901 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-7ca357d9-5029-4bdf-b770-9f6d2125c3c1.parquet
                    79257664 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-81b331e1-d220-4713-aba0-cc93d7d5c9ef.parquet
                    7948 web_site-3701731cfd7045c2bfad58de0a6f1450/metadata/20230630_132904_00778_wcd7b-d0a7e7b2-bc77-4270-821c-148bc7d4eaad.stats
                    79507659 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-1cc8840b-3612-4615-a4b3-772ba9daceb5.parquet
                    79552748 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-60037739-6bb7-4514-a7de-b52c5a3d07cb.parquet
                    79718061 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-27d315f2-b469-4ef8-bd99-be81d263c82f.parquet
                    79736639 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/data/20230630_134903_00788_wcd7b-fe830a85-e4d8-4288-8aa1-1d5b179810c6.parquet
                    7992 customer_demographics-b155cd14884349a4a15b299f4e6cad95/metadata/566a0227-c06d-4c04-8f02-3cfe62a5d676-m0.avro
                    8080 customer-3f927ff1d32d4935aed0180e48f74019/metadata/00001-7f0153bc-230f-4cba-b582-8e3de110b00e.metadata.json
                    8139 promotion-1c49844df9ae466080df3a48d07e30fe/metadata/72740738-7ff8-4aa3-8c63-20211d71f5dc-m0.avro
                    81967657 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-8a3e886f-8302-41ca-b272-8e865ec07adb.parquet
                    82065926 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-d0359249-a428-4488-a20c-291b97a00916.parquet
                    82078889 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-b7d520c4-7807-456f-85cb-1757f680a766.parquet
                    82093366 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-2b569835-403d-41b9-979b-f337dd96d724.parquet
                    82119047 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-669c27cb-4141-4658-ba3e-a22ab0f5cec8.parquet
                    82126724 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-1ef3ee01-7cf7-4243-a148-f6a2268a9de2.parquet
                    82176906 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-4922b444-dacb-492f-8c0a-fad3bcbe3fc6.parquet
                    82180415 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-70245667-2a41-4473-bdce-541bd9e61af0.parquet
                    82195161 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-a7fd854a-ceb5-4773-889e-8c0be8c96b83.parquet
                    82215372 catalog_returns-b374fe5dc5b4463693183f386c251474/data/20230630_132908_00780_wcd7b-636d5897-970b-4984-8f51-a957a866bde8.parquet
                    8243751 web_sales-9485b1bc457a475299e2e43e01c5fa76/data/20230630_135506_00797_wcd7b-fb10aa76-12e2-44d6-a932-e439977199b5.parquet
                    8332 promotion-1c49844df9ae466080df3a48d07e30fe/metadata/00001-8210f7ce-62a6-4ccb-acd9-9c30136e5cf5.metadata.json
                    8359 inventory-04a35aa569bd439cad7cb616a9abde08/metadata/cafd335c-3297-4969-8534-53c7f72d051b-m0.avro
                    8463 item-06036328e0fc4ea9af861e8eccb8a4d0/metadata/82e9d78d-9822-4337-9a3e-7cc80ef118ca-m0.avro
                    86725654 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-8f1a449c-e2bb-422b-8139-41d128b2c7b0.parquet
                    86753223 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-bfc8e4e0-cc37-45f4-812f-24f9a5e33287.parquet
                    8744 date_dim-23d031cec1644a1c8123d56c8a4eafbd/metadata/7a486ff9-1976-49a0-989c-ff0c2c85ccb6-m0.avro
                    877080 web_sales-9485b1bc457a475299e2e43e01c5fa76/metadata/20230630_135506_00797_wcd7b-31c5adc7-c296-4df4-bcf7-116e6210898c.stats
                    8805 web_site-3701731cfd7045c2bfad58de0a6f1450/metadata/7f4869cf-2d18-41a1-a2fb-68f742704877-m0.avro
                    8812 store_returns-ab090d465aaa4eefbe2895aee98c4fcb/metadata/00001-05dcf6b6-efc5-4231-8e63-6f4cfefbfff2.metadata.json
                    88477170 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-f3f77a1d-aa8a-44e6-9168-8ac3e02886a3.parquet
                    891712 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/metadata/20230630_133101_00782_wcd7b-7c333693-c901-477a-898a-90220814fd22.stats
                    8946 customer_address-c3be8509ad5d497aa3d6d18fc72d09cd/metadata/30dddc44-91c4-4e49-88a1-d2e6877bf771-m0.avro
                    8951 call_center-cace8ff8957e41659bd77f8deae79ed6/data/20230630_132737_00748_wcd7b-d21e3452-e082-472f-afec-f3d1bacaf2b0.parquet
                    89861578 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-22ab2261-881a-48de-8529-bc631b9547fb.parquet
                    9023 store-347a9a4c5a194e3d97066fb362f9d087/metadata/e218e600-cffc-4ab1-b6fe-d3ccb8a99985-m0.avro
                    90583489 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-eb5b965a-0485-4cbd-b9d1-e24392c4af7b.parquet
                    909635 date_dim-23d031cec1644a1c8123d56c8a4eafbd/data/20230630_132803_00758_wcd7b-cf5494d6-92dd-4bb9-a53d-b7feb50dede9.parquet
                    9124 call_center-cace8ff8957e41659bd77f8deae79ed6/metadata/5a9cf442-82d0-474a-aadd-c5eed1e3065b-m0.avro
                    9238 item-06036328e0fc4ea9af861e8eccb8a4d0/metadata/00001-423cc82d-9dc3-4958-8b17-262e25b625df.metadata.json
                    9323 call_center-cace8ff8957e41659bd77f8deae79ed6/metadata/20230630_132737_00748_wcd7b-9b63c62a-f3d8-4738-81d1-d34dd9c7438f.stats
                    93544708 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-0f5d713c-fdbd-4ef8-8a67-a5e5e4e2ab41.parquet
                    94853612 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-7d943b13-2721-4411-a2b1-295f88b490b6.parquet
                    97613509 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-00bb8cd2-f7c4-4bb7-9f03-91278027e9bc.parquet
                    9782 store_sales-dbf6ed25869c4fb08651af77f50a9a07/metadata/00001-a730875a-5cdd-414e-a1ba-83796c8ea3e5.metadata.json
                    98415506 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-f66c739e-2e6d-4991-bece-bfb8583ba662.parquet
                    9844 customer-3f927ff1d32d4935aed0180e48f74019/metadata/9add9a6f-22fe-4c3e-864a-6852eb81cad0-m0.avro
                    98611603 catalog_sales-7131536b11774201b7a9b0d87fb7d07b/data/20230630_133101_00782_wcd7b-58e1ab92-ee72-47ae-823a-df69a846277b.parquet
                    99295 time_dim-1a224b45329540558d44b3790118b562/metadata/20230630_132851_00772_wcd7b-8b7a7fa0-31f4-4733-b502-226d9d6ab083.stats
                    """;
            BenchmarkRunner.verifyDataListing(resolveTablesLocation(dataLocation), "Run `testing/benchmark-data/hydrate.sh iceberg-tpcds-sf100` first.", expected);
        }
    }
}
