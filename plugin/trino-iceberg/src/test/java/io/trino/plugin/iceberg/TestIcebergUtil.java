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
package io.trino.plugin.iceberg;

import org.testng.annotations.Test;

import java.util.OptionalInt;

import static io.trino.plugin.iceberg.IcebergUtil.parseVersion;
import static io.trino.testing.assertions.Assert.assertEquals;

public class TestIcebergUtil
{
    @Test
    public void testParseVersion()
    {
        assertEquals(parseVersion("hdfs://hadoop-master:9000/user/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44/metadata/00000-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), OptionalInt.of(0));
        assertEquals(parseVersion("hdfs://hadoop-master:9000/user/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44/metadata/99999-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), OptionalInt.of(99999));
        assertEquals(parseVersion("s3://krvikash-test/test_icerberg_util/orders_93p93eniuw-30fa27a68c734c2bafac881e905351a9/metadata/00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), OptionalInt.of(10));
        assertEquals(parseVersion("/var/test/test_icerberg_util/orders_93p93eniuw-30fa27a68c734c2bafac881e905351a9/metadata/00011-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), OptionalInt.of(11));
        assertEquals(parseVersion("00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), OptionalInt.of(10));

        assertEquals(parseVersion("hdfs://hadoop-master:9000/user/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44"), OptionalInt.empty());
        assertEquals(parseVersion("hdfs://hadoop-master:9000/user/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44/metadata"), OptionalInt.empty());
        assertEquals(parseVersion("hdfs://hadoop-master:9000/user/hive/warehouse/orders_5-581fad8517934af6be1857a903559d44/metadata/00003_409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), OptionalInt.empty());
        assertEquals(parseVersion("s3://krvikash-test/test_icerberg_util/orders_93p93eniuw-30fa27a68c734c2bafac881e905351a9/metadata/00010_409702ba_4735_4645_8f14_09537cc0b2c8.metadata.json"), OptionalInt.empty());
        assertEquals(parseVersion("/var/test/test_icerberg_util/orders_93p93eniuw-30fa27a68c734c2bafac881e905351a9/metadata/-00010-409702ba-4735-4645-8f14-09537cc0b2c8.metadata.json"), OptionalInt.empty());
        assertEquals(parseVersion("orders_5_581fad8517934af6be1857a903559d44"), OptionalInt.empty());
    }
}
