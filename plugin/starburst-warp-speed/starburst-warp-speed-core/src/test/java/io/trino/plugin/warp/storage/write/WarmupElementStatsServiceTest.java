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
package io.trino.plugin.warp.storage.write;

import io.airlift.slice.Slices;
import io.trino.plugin.warp.config.SharedConfig;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.spi.catalog.CatalogName;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.VarcharType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class WarmupElementStatsServiceTest
{
    private static WarmupElementStatsService warmupElementStatsService;

    @BeforeAll
    public static void beforeAll()
    {
        warmupElementStatsService = new WarmupElementStatsService(new ShapingLoggerFactory(new CatalogName("c"), new SharedConfig()));
    }

    @Test
    public void testInteger()
    {
        WarmupElementStats warmupElementStats = new WarmupElementStats(1, 5L, 10L);
        WarmupElementStats result = warmupElementStatsService.getFinalStats(
                IntegerType.INTEGER,
                warmupElementStats,
                RecTypeCode.REC_TYPE_INTEGER,
                WarmUpType.WARM_UP_TYPE_BASIC);
        assertThat(result).isEqualTo(warmupElementStats);
    }

    @Test
    public void testUnInitialize()
    {
        WarmupElementStats warmupElementStats = WarmupElementStats.UNINITIALIZED;
        WarmupElementStats result = warmupElementStatsService.getFinalStats(
                DecimalType.createDecimalType(20, 10),
                warmupElementStats,
                RecTypeCode.REC_TYPE_DECIMAL_LONG,
                WarmUpType.WARM_UP_TYPE_BASIC);
        assertThat(result).isEqualTo(warmupElementStats);
    }

    @Test
    public void testVarchar()
    {
        WarmupElementStats warmupElementStats = new WarmupElementStats(1, Slices.utf8Slice("test"), Slices.utf8Slice("t"));
        WarmupElementStats expectedResult = new WarmupElementStats(1, Slices.utf8Slice("test").toStringUtf8(), Slices.utf8Slice("t").toStringUtf8());
        WarmupElementStats result = warmupElementStatsService.getFinalStats(
                VarcharType.VARCHAR,
                warmupElementStats,
                RecTypeCode.REC_TYPE_VARCHAR,
                WarmUpType.WARM_UP_TYPE_DATA);
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    public void testChar()
    {
        WarmupElementStats warmupElementStats = new WarmupElementStats(1, Slices.utf8Slice("test"), Slices.utf8Slice("t"));
        WarmupElementStats expectedResult = new WarmupElementStats(1, Slices.utf8Slice("test").toStringUtf8(), Slices.utf8Slice("t").toStringUtf8());
        WarmupElementStats result = warmupElementStatsService.getFinalStats(
                CharType.createCharType(10),
                warmupElementStats,
                RecTypeCode.REC_TYPE_VARCHAR,
                WarmUpType.WARM_UP_TYPE_DATA);
        assertThat(result).isEqualTo(expectedResult);
    }

    /**
     * "aaaaaaa a" is longer than STAT_MAX_SLICE_LENGTH and last value is space
     */
    @Test
    public void testCharWithMinValueAsSpace()
    {
        WarmupElementStats warmupElementStats = new WarmupElementStats(1, Slices.utf8Slice("aaaaaaa a"), Slices.utf8Slice("t"));
        byte[] expected = Slices.utf8Slice("aaaaaaa ").byteArray();
        expected[7]--;
        WarmupElementStats expectedResult = new WarmupElementStats(1, Slices.wrappedBuffer(expected).toStringUtf8(), Slices.utf8Slice("t").toStringUtf8());
        WarmupElementStats result = warmupElementStatsService.getFinalStats(
                CharType.createCharType(7),
                warmupElementStats,
                RecTypeCode.REC_TYPE_VARCHAR,
                WarmUpType.WARM_UP_TYPE_DATA);
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    public void testCharWithMaxValueAsSpace()
    {
        byte[] maxValue = Slices.utf8Slice("aaaaaaa  ").byteArray();
        maxValue[7] = 31;
        WarmupElementStats warmupElementStats = new WarmupElementStats(1, Slices.utf8Slice("t"), Slices.wrappedBuffer(maxValue));
        byte[] expected = Slices.utf8Slice("aaaaaaa ").byteArray();
        expected[7]++;
        WarmupElementStats expectedResult = new WarmupElementStats(1, Slices.utf8Slice("t").toStringUtf8(), Slices.wrappedBuffer(expected).toStringUtf8());
        WarmupElementStats result = warmupElementStatsService.getFinalStats(
                CharType.createCharType(7),
                warmupElementStats,
                RecTypeCode.REC_TYPE_VARCHAR,
                WarmUpType.WARM_UP_TYPE_DATA);
        assertThat(result).isEqualTo(expectedResult);
    }
}
