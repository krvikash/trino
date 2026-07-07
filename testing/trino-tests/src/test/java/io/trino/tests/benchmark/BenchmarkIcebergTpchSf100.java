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
 * Iceberg TPC-H entry point at scale factor 100.
 */
public final class BenchmarkIcebergTpchSf100
{
    private BenchmarkIcebergTpchSf100() {}

    static void main(String[] args)
            throws Exception
    {
        System.exit(BenchmarkRunner.run(args, new IcebergTpchSf100Workload(), BenchmarkIcebergTpchSf100.class));
    }

    static final class IcebergTpchSf100Workload
            extends BaseIcebergTpchWorkload
    {
        IcebergTpchSf100Workload()
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
            return DataSize.of(80, DataSize.Unit.GIGABYTE);
        }

        @Override
        public void validateDataLocation(String dataLocation)
        {
            super.validateDataLocation(dataLocation);
            String expected =
                    """
                    102099929 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-28df7129-0f7a-4e1f-ba9f-de0b96ed808e.parquet
                    10482 customer-8826fb2be8434f459a80851a21ce6a48/metadata/40b162f4-78a6-4a75-9842-d18cc173da99-m0.avro
                    105887018 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-6f10ac38-5d00-45cc-95f9-0989874dbc9d.parquet
                    107244196 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-9e90bdff-9e2f-4f91-95a2-52a58d76993b.parquet
                    107784703 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-52d2e3ea-6e4c-41e1-a8de-ce3bc0448b0a.parquet
                    108141896 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-21c666c0-9b20-4587-bcd7-fda16cdc3ce0.parquet
                    108794457 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-49214626-fe01-4a23-ab50-e1d2d3a026d7.parquet
                    109168436 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-a41fe874-3812-4c8b-aca2-c9b39e24b8dc.parquet
                    109777980 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-9f456dfc-8a79-41ac-8a8e-6b177b3899d7.parquet
                    110029251 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-244c0e76-54ee-4291-8835-763bab308b74.parquet
                    112890615 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-ed1f39db-c7cc-4bee-972d-e0499aa3cbda.parquet
                    12162 orders-657a5880542a4d48a5790660fe85bd46/metadata/8320e526-b7ba-424b-afe4-e5e84ac84c1d-m0.avro
                    126500 part-fba8936441f343f09c999254ec10a676/metadata/20230630_132212_00718_wcd7b-2dcd593c-99c1-4665-b2f3-601234978fc8.stats
                    141824389 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-f228b61b-b164-4fa6-b1af-d5b3594c610b.parquet
                    142436000 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-9a85c19a-afc5-4665-920b-aa23bd3da9ad.parquet
                    144355016 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-41433b14-2439-4d2b-9695-c3118ca2b262.parquet
                    144360047 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-69e1f04e-15a9-4f9c-a3d2-ae57392dae5a.parquet
                    144376631 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-18360e4e-8ada-4152-ae11-48c3f1e2a382.parquet
                    146968687 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-b7fc7db2-af96-4bcd-a5d3-d1790075b06f.parquet
                    147262935 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-a39f3b4c-78d6-480a-8357-a04e1026d152.parquet
                    147269626 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-522a8351-8c59-47fc-8796-2700af92b1b0.parquet
                    147999443 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-98a77f84-5670-4b49-b8e4-38b459a8df0a.parquet
                    148314605 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-874caf47-9fe2-4c3f-84f3-8badfaa6dbab.parquet
                    148774459 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-71482870-15ed-4dd8-bf28-01474e24f17c.parquet
                    148878953 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-7143baa7-94b9-4680-9e25-4e16fd4dca19.parquet
                    149456414 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-1e4767f3-1e26-44a9-98ef-af73b869d8aa.parquet
                    150014132 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-07e640d7-c390-49bc-a33a-6abbce6170c0.parquet
                    150030391 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-625eba3c-127b-4e02-8ede-0b9d44327f45.parquet
                    150710556 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-64872a16-060a-4dcd-9e10-6972b32fd48f.parquet
                    151156279 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-eee247eb-d89c-4c48-9560-249a6581d994.parquet
                    151609571 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-482610fd-7915-48ef-86cd-f62bf691b9db.parquet
                    152414129 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-fbd38f5a-c9a5-4b47-bd20-21b65a604c71.parquet
                    153153040 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-47bcec81-a894-4e60-963d-090ab0292695.parquet
                    154980 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/metadata/20230630_132223_00722_wcd7b-0a2593c2-359d-4169-9954-20fb723645e2.stats
                    1575 nation-22ecbf33f3d64878ad7ef9fcb738c5a3/metadata/20230630_132135_00707_wcd7b-3d52a4f3-11fb-4008-89ab-06fb19149dab.stats
                    16018941 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-8dd19f00-ef1f-4195-8b0d-27b9614d35db.parquet
                    1613857 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-6f73a98a-6f35-473c-93aa-c10820d93f30.parquet
                    163612774 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-48d1bcc9-9f40-49de-86c0-510fb183a3b3.parquet
                    167098123 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-e6270266-7d93-48e5-a3a0-e1b559e4903e.parquet
                    168772749 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-d1df223b-40c8-4157-b8bc-6d482ddf692c.parquet
                    169167 orders-657a5880542a4d48a5790660fe85bd46/metadata/20230630_132138_00709_wcd7b-0f643c18-dd45-4220-9d15-bd30a394a27d.stats
                    173011798 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-328f395b-1f7d-44f4-893b-41a78d43f598.parquet
                    176919131 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-dfe95e98-5a71-4c05-86a6-c0e0cadc8a0c.parquet
                    177920384 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-d0ccf319-ce27-4147-aaab-0e7ff9fc0e49.parquet
                    180123519 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-90fcc7a9-d1ac-484a-a75f-76a87c4edd71.parquet
                    180569006 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-07e320ec-e137-42e1-a5bb-ccec4fe69798.parquet
                    181674973 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-fbb4a53a-f8aa-4ee7-8cec-f16da24c42b7.parquet
                    181974248 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-8e7539d6-88a2-4ae5-bb5c-0825728f0169.parquet
                    182381604 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-6d96917f-48ab-4e0e-99fe-49fbb310b7db.parquet
                    182401771 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-a99e935e-62ec-4374-979a-6338c3a29e08.parquet
                    183067 customer-8826fb2be8434f459a80851a21ce6a48/metadata/20230630_131952_00703_wcd7b-25c74713-deae-48a7-87ca-91ad3f003940.stats
                    186192180 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-518b8b6b-34a1-4c3e-a697-470f19578cce.parquet
                    186235411 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-040d9d3c-1312-4362-bb6d-1b6c951c1e71.parquet
                    186481088 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-b59c7f35-a462-470b-bafc-610c1a7814f1.parquet
                    187064 supplier-72a65d865ba241cf9541831ff8e18fb5/metadata/20230630_132247_00734_wcd7b-e5795732-a537-4b26-9873-a7ae545f48be.stats
                    187506446 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-87fa06e2-380d-4e68-a278-924ce114f178.parquet
                    187573896 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-10411ac8-e6ef-4720-9f80-ec594590eeb9.parquet
                    1890 nation-22ecbf33f3d64878ad7ef9fcb738c5a3/data/20230630_132135_00707_wcd7b-b81638ec-ecc3-4a73-ba6d-b8b8d1c3ea3d.parquet
                    190864871 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-2e5ff034-b5d0-46d4-b23d-d6c4c5e5eff5.parquet
                    193394112 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-ea6ed212-46b9-42e2-b665-a9dbd83247c2.parquet
                    19351 lineitem-5817376144ae4e569d71eb95f6867db2/metadata/a6bf2184-665c-43fd-bf8e-097a1fccafda-m0.avro
                    194690685 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-e70ac766-eb35-417b-8f1a-6ae6e683e469.parquet
                    206035 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-3e6ecf94-ba4c-4b0d-9441-3c41c0f15df5.parquet
                    206228 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-2a5a6b0f-d073-4914-b141-7c0eac9b4a70.parquet
                    20873260 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-7787be5e-be76-4b15-93fa-c3c6808a42e1.parquet
                    20938701 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-3168459e-de9c-4b08-96b5-d33cfae7e62c.parquet
                    2097 region-74f93ec72c87444992b69b67aa125bd1/metadata/00000-0b1b835e-26b4-40cb-b3b3-7a6d18013d2e.metadata.json
                    21088893 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-8a58bda2-c5da-4cf8-9045-42bd13271b5a.parquet
                    21091593 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-4be01a63-975a-42b1-bd6f-f257b22c2c6f.parquet
                    211800 lineitem-5817376144ae4e569d71eb95f6867db2/metadata/20230630_132002_00704_wcd7b-e7f30a69-2c6c-429b-94d6-b4b379f9eaf3.stats
                    21186464 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-d6785ae5-8eec-4c46-8351-1402a4c8dbf6.parquet
                    21224193 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-9c8be2da-6a35-435b-91d1-e181aa95ff80.parquet
                    21583554 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-72ad536c-7d80-41c7-888d-f8422f3d8c65.parquet
                    21649222 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-95f7a260-0de9-414a-809f-68770bea5667.parquet
                    21951663 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-0f3686b3-bb7e-43f9-b5ce-72bb6c0fb4e3.parquet
                    21958199 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-12500df8-f251-4ca2-869f-c62fcd5d9856.parquet
                    22001262 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-39b4f350-5ac0-4781-a613-e17d27bea440.parquet
                    2202 nation-22ecbf33f3d64878ad7ef9fcb738c5a3/metadata/00000-edf48121-b09b-479f-b8d4-c8a8e78ad966.metadata.json
                    22309112 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-87fa1157-33c6-48c6-81d6-52816190aefc.parquet
                    22512037 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-4f0e5d26-cbb6-4d35-bbb4-86b74fffc8b0.parquet
                    23033927 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-15a4a1b4-0417-42ef-896a-bf24318291d8.parquet
                    2341 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/metadata/00000-d4810f96-ad19-4e45-b9b9-3e98cde9f1fb.metadata.json
                    23736443 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-b0a2741c-da56-47e1-b5fb-2ec4d65045de.parquet
                    23832418 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-8b0fb4ac-10ce-4397-a24a-ad8c26fddb69.parquet
                    240183 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-2266e419-9b56-47b1-b94e-bdb0673dcf2b.parquet
                    2533 supplier-72a65d865ba241cf9541831ff8e18fb5/metadata/00000-2a5542e4-3144-4e9b-964e-9b90cbdece45.metadata.json
                    25332439 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-68a155bc-b8a9-4cde-9ea2-5339b23cfcae.parquet
                    2641 customer-8826fb2be8434f459a80851a21ce6a48/metadata/00000-721e6a40-c0ff-4e7d-a44c-85ae0199614f.metadata.json
                    271080501 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-4249706b-c121-458c-ae6a-1f117993bce0.parquet
                    2725 part-fba8936441f343f09c999254ec10a676/metadata/00000-e129392a-e944-4a46-8777-0ab8003c7e25.metadata.json
                    273402565 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-45ab98cd-c46d-4b2e-8775-1091fda06ce1.parquet
                    274855203 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-b91c8422-703d-42ab-a214-3fbc4a86aa1c.parquet
                    274918049 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-019857ab-4ed3-4830-a97b-9722d36b2b67.parquet
                    275546113 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-4742365d-2113-4a58-a675-10cb4e349c13.parquet
                    2756 orders-657a5880542a4d48a5790660fe85bd46/metadata/00000-e5bbe13b-ef86-433c-aae4-fdcf945fe90d.metadata.json
                    275978981 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-5ee35970-27f1-499c-aef3-80c12e329a78.parquet
                    276012032 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-4d0eb8ed-09fa-4b85-ab81-6d32edafb7be.parquet
                    276217708 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-aa2d42f7-e010-497c-a752-e60bd74ef33d.parquet
                    277670050 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-55bb0578-3b63-4202-a8ce-6ab6cc6cdf2a.parquet
                    278939471 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-b16895e4-dd3b-4bbf-a1ea-d894de9a9c26.parquet
                    28358766 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-1d78c1a7-dfed-4f28-bbeb-810ccb40d99b.parquet
                    321171566 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-68458d87-b356-4df9-930f-dec82a7f6b17.parquet
                    321651662 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-45fc9ad4-6785-449f-b2cf-104d357e1306.parquet
                    3284 region-74f93ec72c87444992b69b67aa125bd1/metadata/00001-102e9ed0-d5a3-4bb9-813b-2daa1a659ab7.metadata.json
                    330236532 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-9b947246-5c76-4655-a1c0-f778118ebf9f.parquet
                    331483038 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-2be3deb0-a2ef-4629-9bd3-e7c552373f14.parquet
                    331752277 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-8343a4d9-53f7-4274-b603-0951c738b6be.parquet
                    332527319 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-f4596197-be82-4e05-81c8-7da13f6a9fdf.parquet
                    3331012 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-9b79127d-deca-41cc-8b66-a7b7fcde3276.parquet
                    335562354 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-913b9350-f298-4325-a2d9-337c32051906.parquet
                    339289835 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-81386de8-fcf4-47fd-b941-a03862b6512c.parquet
                    339296549 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-cdada17c-a026-4cb5-b7c2-2fa9268469ae.parquet
                    341073520 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-07e49efc-314f-446b-8551-516715e80f7d.parquet
                    34922050 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-90d6e7e0-2ff5-407f-ae7e-c5d8c3ffc8ad.parquet
                    3504 lineitem-5817376144ae4e569d71eb95f6867db2/metadata/00000-9704520b-7ed7-4230-83a2-f99c8b43deca.metadata.json
                    3581917 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-1f4ee2ae-5077-4e80-9763-2bfed995e788.parquet
                    3596 nation-22ecbf33f3d64878ad7ef9fcb738c5a3/metadata/00001-5c729a52-0b15-49fa-81b6-3a3a2d6298b0.metadata.json
                    359941733 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-8f8bc835-e22f-4b25-b08e-647b5e84ccd1.parquet
                    360261379 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-52db884b-8fe6-4092-8514-ef924d6a3261.parquet
                    36065117 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-817db9c7-9c25-428b-b528-391d7ce7a73d.parquet
                    364882227 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-6b912ec2-dbdc-4ce9-aca4-0198db22d8df.parquet
                    365935418 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-9bb80319-0b3e-46a3-8a86-7134942d4bd3.parquet
                    368339243 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-55249210-13fe-4f54-b8c4-ed457e3a9405.parquet
                    36937645 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/data/20230630_132223_00722_wcd7b-29f09906-0b40-4c83-ba28-ca65af62f305.parquet
                    369579831 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-d90ac7d2-8f9b-4adc-8123-bc80c32c003c.parquet
                    372498545 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-b44db622-ffea-46b7-96d1-7191f54b4558.parquet
                    37337577 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-13f74b56-35a4-40e3-9e90-0c7e5dc6370f.parquet
                    37531540 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-89547960-594b-4b9e-a88a-df84fb217141.parquet
                    376481075 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-44f78f3d-fe27-419d-b7d1-4e1dedd63ba7.parquet
                    37826549 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-af8768b6-248e-44c3-b99f-6234af7e8432.parquet
                    37966349 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-3ee59a54-76ef-4ed8-b60f-4c01270cc4da.parquet
                    37978720 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-f9922e43-c802-4a60-9769-d32e7ea93524.parquet
                    38112480 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-57e0938c-2542-49af-b7e7-3cf18bcc3377.parquet
                    38142485 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-798b15c2-3ae5-469e-9555-dccb02887308.parquet
                    38150072 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-330657e1-b219-43b5-a547-c20e19126849.parquet
                    38172714 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-b2d1d951-7dfd-428a-8531-cc9ff3a8aa0a.parquet
                    381983043 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-92668a0b-a6ee-4cc2-b4f3-47a632181f10.parquet
                    382128039 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-bb215892-4a9c-4cbf-884f-d039c5223d8a.parquet
                    38534570 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-85b68679-3040-4c7a-985f-b7715b011497.parquet
                    388781385 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-412fdaf8-0e1f-4e31-9d46-6f2f456c87d9.parquet
                    390593834 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-9e4c340e-75b3-4caa-b091-d96a8de94f95.parquet
                    393433 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-6cb0535c-fb19-4676-8b32-7c720768fd02.parquet
                    396215574 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-a9c597db-644c-40e0-a073-997e165cfdf9.parquet
                    3971 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/metadata/00001-06e7ce1c-b6cc-478a-84d7-6c3aaea4833d.metadata.json
                    399161404 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-a9452ecd-7ab3-4d62-8c5c-5b950b81c83a.parquet
                    400375757 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-47d164d0-c391-41b5-add7-99583503d9e9.parquet
                    400788689 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-896c1d60-cc34-4c94-87ae-8c086d82c3a2.parquet
                    406162950 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-f9c08842-0312-4d69-a800-69a3f7c19446.parquet
                    407823606 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-eb932a8a-fab0-408f-87e3-80ccb4c72171.parquet
                    411552396 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-5cf015ac-005c-4ff0-8830-88243b18c4ac.parquet
                    412462987 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-002dd5b4-0462-4b06-a009-ffb628cbf637.parquet
                    41998096 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-9702bcc9-22e0-4f26-b84b-252b4885c573.parquet
                    4265317 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-64da0c8e-2a81-4bc8-ab01-547641eadfa7.parquet
                    4300 region-74f93ec72c87444992b69b67aa125bd1/metadata/snap-3868868756269454785-1-ce49205c-fc97-4cca-93f8-ecaf90cebdb3.avro
                    4301 nation-22ecbf33f3d64878ad7ef9fcb738c5a3/metadata/snap-6806274954947537894-1-bb21ef3d-4c95-4e73-977f-4d2f7267063c.avro
                    4307 part-fba8936441f343f09c999254ec10a676/metadata/snap-9165572167052717914-1-fb2e93d3-0925-4085-8d58-5ff8bbfabb8a.avro
                    4308 orders-657a5880542a4d48a5790660fe85bd46/metadata/snap-8356586832903826859-1-8320e526-b7ba-424b-afe4-e5e84ac84c1d.avro
                    4310 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/metadata/snap-5041928270006315843-1-eacf92fc-743f-4eca-a25f-77c70bfb4f8f.avro
                    4311 customer-8826fb2be8434f459a80851a21ce6a48/metadata/snap-6406078200810403408-1-40b162f4-78a6-4a75-9842-d18cc173da99.avro
                    4312 lineitem-5817376144ae4e569d71eb95f6867db2/metadata/snap-1002787302317224860-1-a6bf2184-665c-43fd-bf8e-097a1fccafda.avro
                    4312 supplier-72a65d865ba241cf9541831ff8e18fb5/metadata/snap-5099149170167496315-1-b44bcc84-4819-41c7-a088-d88c2114c395.avro
                    4447173 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-9ba755fa-8dc5-49ad-8726-9176b00119fd.parquet
                    45418421 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-12143cbe-4bd6-4828-83fe-9cd781af74fa.parquet
                    454977 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-9634d4b2-95ad-4f75-8221-cddad4c13b71.parquet
                    4573 supplier-72a65d865ba241cf9541831ff8e18fb5/metadata/00001-f8df828d-9c78-411e-b256-13600066e128.metadata.json
                    4595176 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-a0852a38-5e50-44ee-adfe-e5b8a5ccd4a7.parquet
                    4612317 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-f7f09571-6d7a-4a69-add6-644cce16e5f8.parquet
                    4619289 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-b4a0c695-3a47-4340-9073-b88d311c75dc.parquet
                    4693614 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-00d76dd1-3a92-488c-8d4d-05be9a1bf215.parquet
                    4700351 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-2156ada9-63ed-4886-bf4c-e8fc66289e14.parquet
                    4812696 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-24c0251d-d87b-455e-bd56-c1d9173e5ca0.parquet
                    48356653 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-015becdc-2b5c-4c2d-a89a-ecfdfa425c95.parquet
                    4874766 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-f75c048f-1e7b-4f23-adde-6e0b6178977b.parquet
                    4893 customer-8826fb2be8434f459a80851a21ce6a48/metadata/00001-df5128eb-cde2-4b2d-aab4-674046e67d3f.metadata.json
                    50365646 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-e54e5991-b409-450e-b188-5a46f9a21348.parquet
                    5098383 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-7e207c04-c168-4281-8dbe-cd1b2341efb0.parquet
                    5160 part-fba8936441f343f09c999254ec10a676/metadata/00001-703bf298-4864-4224-b4b6-dcdbffde5fd3.metadata.json
                    5163145 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-92246123-8c47-4a61-b430-5af5d7497f32.parquet
                    5166174 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-abd1c755-9adf-4b56-903f-2faed8803216.parquet
                    5166518 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-619250b6-ea6c-4a28-bc39-dc4f1ac4fa9f.parquet
                    5167069 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-afe851c1-e318-4011-b503-fb766d5691f5.parquet
                    5167910 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-7a36889e-682a-4b26-b409-31125657a2e6.parquet
                    5169408 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-648c4e7b-8eb5-4514-b917-64ad6dd99a3e.parquet
                    5175194 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-43dd7f1d-cde3-48e8-9387-c93dc1d87122.parquet
                    5177638 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-24a334c4-0780-4246-8185-e67778700530.parquet
                    5182510 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-55a762d2-3460-415e-990e-a9e7d18e0f74.parquet
                    5199833 supplier-72a65d865ba241cf9541831ff8e18fb5/data/20230630_132247_00734_wcd7b-99c9ba2f-7c63-4e51-ac74-2ba7d5771e4a.parquet
                    5203 orders-657a5880542a4d48a5790660fe85bd46/metadata/00001-3ea73194-b9a4-4faa-93e3-0d6cd4912523.metadata.json
                    5217128 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-bedee4a6-c225-4a40-a629-eef02afa3676.parquet
                    5464264 part-fba8936441f343f09c999254ec10a676/data/20230630_132212_00718_wcd7b-636adfc8-4572-49ed-b457-fd982cc55889.parquet
                    55233367 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-a376dcfa-f266-4473-9d43-1e6c69622026.parquet
                    58508133 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-5e2c33d8-d415-4ca0-b41e-3d2ca72e19d0.parquet
                    58740961 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-bb05c8a6-4249-4bbc-9d60-97757a7eb9fc.parquet
                    58842641 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-b7b02058-c115-4711-b49f-a391c732f7db.parquet
                    58915460 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-0a2bf732-2f4e-4db8-816a-463a806611b5.parquet
                    59000382 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-e834d911-4561-4c2d-9b72-74da8f3cb261.parquet
                    59028287 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-fc76a439-9896-4aa3-9bb7-de2667c2367b.parquet
                    59028419 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-d1fd0e69-22e6-4c03-81f4-c8b951a768bd.parquet
                    59337381 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-310be863-9cfe-40f1-a2e5-d0c3857664e5.parquet
                    59776262 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-5e7f1030-1dc1-4ad3-bf04-303585f824ed.parquet
                    59840604 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-0ddeb301-a08f-4b7a-8080-0e9cc08046d7.parquet
                    59916865 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-dc67e82c-009b-47c2-a51d-e8a7081992c2.parquet
                    60051697 customer-8826fb2be8434f459a80851a21ce6a48/data/20230630_131952_00703_wcd7b-0db26a13-802a-4503-a3d0-afdaa7ed8e9c.parquet
                    67013773 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-4242d43f-4fff-463e-8026-5ea31a1084f2.parquet
                    6851 region-74f93ec72c87444992b69b67aa125bd1/metadata/ce49205c-fc97-4cca-93f8-ecaf90cebdb3-m0.avro
                    68903003 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-3b0d53bd-8c66-4c1b-b4e0-05ffc38b1238.parquet
                    69111959 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-e94f6510-b40d-4d98-8cdc-cbd6d2459e05.parquet
                    6921 nation-22ecbf33f3d64878ad7ef9fcb738c5a3/metadata/bb21ef3d-4c95-4e73-977f-4d2f7267063c-m0.avro
                    70132049 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-a76b711c-c282-4351-827b-867e6ef3aac2.parquet
                    70374984 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-12329fd3-928a-4929-a99c-e3bbc099a817.parquet
                    70843685 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-ada08eab-a47d-4b36-b818-42edc306498b.parquet
                    7391 lineitem-5817376144ae4e569d71eb95f6867db2/metadata/00001-1165306b-e340-4bcb-9c4e-ef8015c5f9d5.metadata.json
                    74040573 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-812da784-63ec-4a6a-afe0-e17f25b05b85.parquet
                    74795959 lineitem-5817376144ae4e569d71eb95f6867db2/data/20230630_132002_00704_wcd7b-4b14280c-1e04-4d35-bf82-641206a0a0a8.parquet
                    76940159 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-b5b729ad-926b-4b9f-b5ca-4ecbf144a65a.parquet
                    78478562 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-6ea5dd11-0e04-4b11-9e23-54c10457acf0.parquet
                    79126032 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-216cc0af-da25-44df-b1dd-369746403327.parquet
                    835 region-74f93ec72c87444992b69b67aa125bd1/metadata/20230630_132243_00732_wcd7b-ac89176a-cc3e-41fb-bf7d-f981a891c59c.stats
                    8527 supplier-72a65d865ba241cf9541831ff8e18fb5/metadata/b44bcc84-4819-41c7-a088-d88c2114c395-m0.avro
                    872 region-74f93ec72c87444992b69b67aa125bd1/data/20230630_132243_00732_wcd7b-784336b5-4a36-4025-b54b-7ac9fe9d0302.parquet
                    9567 orders-657a5880542a4d48a5790660fe85bd46/data/20230630_132138_00709_wcd7b-5705584a-6605-4eb1-89b3-22ec64c08dc3.parquet
                    9802 part-fba8936441f343f09c999254ec10a676/metadata/fb2e93d3-0925-4085-8d58-5ff8bbfabb8a-m0.avro
                    9805 partsupp-97b9c1cd18f34aa5bc7574e35e6a77e3/metadata/eacf92fc-743f-4eca-a25f-77c70bfb4f8f-m0.avro
                    """;
            BenchmarkRunner.verifyDataListing(resolveTablesLocation(dataLocation), "Run `testing/benchmark-data/hydrate.sh iceberg-tpch-sf100` first.", expected);
        }
    }
}
