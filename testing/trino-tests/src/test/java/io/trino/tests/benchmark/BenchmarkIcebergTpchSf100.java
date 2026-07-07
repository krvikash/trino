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
                    1073568568 orders-16331d4eeedf4e0c993faceafff8b2e3/data/20260605_115800_00002_i8pqf-2c1062ce-5235-4ee4-83ab-7018426ddd0c.parquet
                    1073575405 customer-f1db0087172d4e90a7eab0f5cde4774f/data/20260605_115742_00001_i8pqf-646d7439-c8c4-4ba1-b668-dd7e577ce08b.parquet
                    1073577626 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-921efe7b-b318-415e-966a-961a3a5ed3d6.parquet
                    1073581962 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-abd0d0b4-4a0a-49f6-8aef-8901f46dd4fe.parquet
                    1073594831 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-438bdf17-72ec-4e2b-b4b4-d8786718d0b4.parquet
                    1073611511 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-3ab21724-0a77-4c13-8980-0a36f4bfa306.parquet
                    1073615901 orders-16331d4eeedf4e0c993faceafff8b2e3/data/20260605_115800_00002_i8pqf-b9db2d59-3f68-48c7-aae2-1111f4b1c74b.parquet
                    1073616947 orders-16331d4eeedf4e0c993faceafff8b2e3/data/20260605_115800_00002_i8pqf-96c0936c-5074-4e03-9b80-e1523e644bb3.parquet
                    1073625762 orders-16331d4eeedf4e0c993faceafff8b2e3/data/20260605_115800_00002_i8pqf-0859a8ea-8166-4ac5-b20d-de78db5a7e06.parquet
                    1073625883 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-1c698fc7-c7ea-45ba-953f-53b87e90fab2.parquet
                    1073629618 orders-16331d4eeedf4e0c993faceafff8b2e3/data/20260605_115800_00002_i8pqf-e6386093-bd96-41f0-a783-6054f0dd8cfa.parquet
                    1073643321 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-0421bdc1-d8fb-458f-9949-1c61b808f6fe.parquet
                    1073648989 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-f3478937-7fe8-433f-8255-a675ff2e07ae.parquet
                    1073655720 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-6a4321dc-c998-42de-8ec5-183aa1d9de03.parquet
                    1073655963 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-7577b68d-6403-429b-a8d1-35e9cd223ead.parquet
                    1073664503 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-74dce19b-f19f-492a-95af-1d3374df91aa.parquet
                    1073665327 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-6cf2431a-473a-4f50-b773-ec9d1cb6a086.parquet
                    1073675435 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-9ccbab82-5f50-480a-8788-d99749b70d61.parquet
                    1073680031 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-9a9bf063-d11b-4eb0-a19a-ca272cd81e34.parquet
                    1073680454 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-b0ae2542-260e-411d-8e36-7f2de2ae1d0e.parquet
                    1073692968 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-cdeb384f-5e17-4f23-9fc7-ed5389750b82.parquet
                    1073696992 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-bb7b5ca9-a92e-4716-911f-b360fd93a812.parquet
                    1073738521 orders-16331d4eeedf4e0c993faceafff8b2e3/data/20260605_115800_00002_i8pqf-2a42bf69-5966-40c4-b55d-3bfade9112c7.parquet
                    1073753679 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-67b734a4-6f2a-41e3-97c7-25be5c906c65.parquet
                    1073791716 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-b16b00fb-f5b0-4d51-8ccc-a3cac828ae76.parquet
                    1073797804 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-fe4da2f8-d8dd-4468-ab80-0476cc3b8777.parquet
                    1073810613 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-ac483699-110a-475e-9722-92bba2a1845d.parquet
                    1073812121 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-8af9d243-7c5c-452d-b662-f609b169ae7a.parquet
                    1074583994 partsupp-befa29c04a7042b89bbf622486705a7c/data/20260605_121859_00005_i8pqf-2b37e9a2-2d1e-4345-9b00-9f4b55d573e0.parquet
                    1074603817 partsupp-befa29c04a7042b89bbf622486705a7c/data/20260605_121859_00005_i8pqf-1727df61-1a55-4cb3-a954-603365532a30.parquet
                    1074858779 partsupp-befa29c04a7042b89bbf622486705a7c/data/20260605_121859_00005_i8pqf-2f2362f0-0b96-4fe8-943f-ec2582f2d3d9.parquet
                    12573 lineitem-c3e45c03224643b3a63c478aaae57b08/metadata/0b228be6-2079-4044-bfd3-d985eac9d0b5-m0.avro
                    126150 part-564b7204508e42de87afa69704a0be7a/metadata/20260605_121830_00004_i8pqf-ea1b1792-f6a9-4d03-abea-18f5007f349d.stats
                    155112 partsupp-befa29c04a7042b89bbf622486705a7c/metadata/20260605_121859_00005_i8pqf-d25548b4-747a-4a4b-b775-bd6cc592151c.stats
                    1583 nation-f54e78db9ef64cc58bf83431ab13b67e/metadata/20260605_122023_00007_i8pqf-10392cc6-3803-4bf6-9dcf-d5a2ff84e506.stats
                    161306500 customer-f1db0087172d4e90a7eab0f5cde4774f/data/20260605_115742_00001_i8pqf-a0d8a136-e89d-4112-a34e-802b1d97a198.parquet
                    169378 orders-16331d4eeedf4e0c993faceafff8b2e3/metadata/20260605_115800_00002_i8pqf-55292387-9389-43e3-87f1-a2ace1342765.stats
                    182318 customer-f1db0087172d4e90a7eab0f5cde4774f/metadata/20260605_115742_00001_i8pqf-a37a28cf-6558-41ff-9cb7-cac77cce4873.stats
                    186299 supplier-e61f2fa498264d49b3711c95e38aa619/metadata/20260605_122021_00006_i8pqf-e94d755e-3296-46b3-9039-9277858a3db2.stats
                    211655 lineitem-c3e45c03224643b3a63c478aaae57b08/metadata/20260605_120041_00003_i8pqf-cd9d3e76-53ad-4af9-af20-d3fdcc93ea63.stats
                    2329 nation-f54e78db9ef64cc58bf83431ab13b67e/data/20260605_122023_00007_i8pqf-941b8825-07e5-4c36-96f0-2aca9d8f2738.parquet
                    2411 region-c8f62a399e354427a2b5eb5dee904da5/metadata/00000-4d5b80f0-a1e9-4196-9c9a-adc6259c7c7e.metadata.json
                    2619 nation-f54e78db9ef64cc58bf83431ab13b67e/metadata/00000-aeef1061-0a00-4aa2-9efb-4ef76cfa777d.metadata.json
                    26954693 orders-16331d4eeedf4e0c993faceafff8b2e3/data/20260605_115800_00002_i8pqf-b59e88a5-cae2-44a7-b76a-795ca919fada.parquet
                    2877 partsupp-befa29c04a7042b89bbf622486705a7c/metadata/00000-83d3097f-774b-402b-b8c3-eb56c9c1173e.metadata.json
                    3253 supplier-e61f2fa498264d49b3711c95e38aa619/metadata/00000-d6d16496-6d4e-4af6-867a-3b4ed1ccd172.metadata.json
                    3462 customer-f1db0087172d4e90a7eab0f5cde4774f/metadata/00000-4e519c7f-3a84-4270-a3ae-7e07fd09703e.metadata.json
                    3618 part-564b7204508e42de87afa69704a0be7a/metadata/00000-acdf7e4a-2851-46d7-9ef0-ad9497984d3d.metadata.json
                    3659 orders-16331d4eeedf4e0c993faceafff8b2e3/metadata/00000-1195da76-f547-4d8c-a3b2-8328e1b99cac.metadata.json
                    43687063 lineitem-c3e45c03224643b3a63c478aaae57b08/data/20260605_120041_00003_i8pqf-5b3e23cb-6db1-4728-83db-e55b80db42c6.parquet
                    4459 orders-16331d4eeedf4e0c993faceafff8b2e3/metadata/snap-2073231875508281729-1-804a0a76-5660-46d0-a1b5-095e10469d6b.avro
                    4459 region-c8f62a399e354427a2b5eb5dee904da5/metadata/snap-216269885248604448-1-4cfba42a-5b1a-4456-be94-0e048fca5d13.avro
                    4461 nation-f54e78db9ef64cc58bf83431ab13b67e/metadata/snap-5239597519111826465-1-9ec25108-ea73-4ad9-b807-d0505df83e7e.avro
                    4463 part-564b7204508e42de87afa69704a0be7a/metadata/snap-6537517509113467754-1-dbcd53e6-73ab-4af3-bb18-d7620c50ee1b.avro
                    4467 partsupp-befa29c04a7042b89bbf622486705a7c/metadata/snap-2018826843109567038-1-4419b6dd-7e12-4590-b9f9-cdec00c9b495.avro
                    4467 supplier-e61f2fa498264d49b3711c95e38aa619/metadata/snap-6323873642306197298-1-4a2142c9-1979-4bd4-bdbe-c1c20c52898d.avro
                    4470 customer-f1db0087172d4e90a7eab0f5cde4774f/metadata/snap-2559268190694483654-1-f0f00d76-4ff9-4c86-9a25-8ac1a1eab719.avro
                    4471 lineitem-c3e45c03224643b3a63c478aaae57b08/metadata/snap-613321520126731423-1-0b228be6-2079-4044-bfd3-d985eac9d0b5.avro
                    5034 lineitem-c3e45c03224643b3a63c478aaae57b08/metadata/00000-a17be025-55bf-40fd-8928-916dc5f7445e.metadata.json
                    616066650 part-564b7204508e42de87afa69704a0be7a/data/20260605_121830_00004_i8pqf-652908e8-cd32-4bb9-8268-026d10f115e9.parquet
                    7179 region-c8f62a399e354427a2b5eb5dee904da5/metadata/4cfba42a-5b1a-4456-be94-0e048fca5d13-m0.avro
                    7252 nation-f54e78db9ef64cc58bf83431ab13b67e/metadata/9ec25108-ea73-4ad9-b807-d0505df83e7e-m0.avro
                    7550 supplier-e61f2fa498264d49b3711c95e38aa619/metadata/4a2142c9-1979-4bd4-bdbe-c1c20c52898d-m0.avro
                    7745 part-564b7204508e42de87afa69704a0be7a/metadata/dbcd53e6-73ab-4af3-bb18-d7620c50ee1b-m0.avro
                    7818 partsupp-befa29c04a7042b89bbf622486705a7c/metadata/4419b6dd-7e12-4590-b9f9-cdec00c9b495-m0.avro
                    78937095 supplier-e61f2fa498264d49b3711c95e38aa619/data/20260605_122021_00006_i8pqf-31f8d99e-9e9c-4498-8230-b5c35507e15b.parquet
                    7901 customer-f1db0087172d4e90a7eab0f5cde4774f/metadata/f0f00d76-4ff9-4c86-9a25-8ac1a1eab719-m0.avro
                    840 region-c8f62a399e354427a2b5eb5dee904da5/metadata/20260605_122023_00008_i8pqf-34344471-e992-44ec-a85b-ddd22852fc8c.stats
                    8707 orders-16331d4eeedf4e0c993faceafff8b2e3/metadata/804a0a76-5660-46d0-a1b5-095e10469d6b-m0.avro
                    938 region-c8f62a399e354427a2b5eb5dee904da5/data/20260605_122023_00008_i8pqf-3a9ad445-5549-45a7-bf16-eb30f2355d80.parquet
                    967676303 partsupp-befa29c04a7042b89bbf622486705a7c/data/20260605_121859_00005_i8pqf-a10d2766-72c0-421c-a513-ceab96820eec.parquet
                    """;
            BenchmarkRunner.verifyDataListing(resolveTablesLocation(dataLocation), "Run `testing/benchmark-data/hydrate.sh iceberg-tpch-sf100` first.", expected);
        }
    }
}
