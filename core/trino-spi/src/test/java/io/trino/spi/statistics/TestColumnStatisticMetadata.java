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
package io.trino.spi.statistics;

import io.airlift.json.JsonCodec;
import io.trino.spi.expression.FunctionName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnStatisticMetadata
{
    private static final JsonCodec<ColumnStatisticMetadata> CODEC = jsonCodec(ColumnStatisticMetadata.class);

    @Test
    public void testRoundTripWithoutFieldPath()
    {
        assertRoundTrip(new ColumnStatisticMetadata("col", NUMBER_OF_DISTINCT_VALUES));
        assertRoundTrip(new ColumnStatisticMetadata("col", "agg_id", NUMBER_OF_DISTINCT_VALUES));
        assertRoundTrip(new ColumnStatisticMetadata("col", "agg_id", new FunctionName("my_agg")));

        // An empty path is written rather than omitted, so fromJson never sees a null fieldPath
        assertThat(CODEC.toJson(new ColumnStatisticMetadata("col", NUMBER_OF_DISTINCT_VALUES)))
                .contains("\"fieldPath\"");
    }

    @Test
    public void testRoundTripWithFieldPath()
    {
        assertRoundTrip(new ColumnStatisticMetadata("attrs", List.of("intent"), "agg_id", new FunctionName("my_agg")));
        assertRoundTrip(new ColumnStatisticMetadata("a", List.of("b", "c", "d"), "agg_id", new FunctionName("my_agg")));
    }

    /**
     * The field path is part of the identity: the same leaf name under different columns, and
     * different leaves under the same column, must not collide as map keys.
     */
    @Test
    public void testFieldPathParticipatesInEquality()
    {
        ColumnStatisticMetadata attrsIntent = new ColumnStatisticMetadata("attrs", List.of("intent"), "agg_id", new FunctionName("my_agg"));
        ColumnStatisticMetadata attrsProduct = new ColumnStatisticMetadata("attrs", List.of("product"), "agg_id", new FunctionName("my_agg"));
        ColumnStatisticMetadata keyIntent = new ColumnStatisticMetadata("key", List.of("intent"), "agg_id", new FunctionName("my_agg"));
        ColumnStatisticMetadata attrsBare = new ColumnStatisticMetadata("attrs", "agg_id", new FunctionName("my_agg"));

        assertThat(attrsIntent).isNotEqualTo(attrsProduct);
        assertThat(attrsIntent).isNotEqualTo(keyIntent);
        assertThat(attrsIntent).isNotEqualTo(attrsBare);
        assertThat(attrsIntent).isEqualTo(new ColumnStatisticMetadata("attrs", List.of("intent"), "agg_id", new FunctionName("my_agg")));
    }

    private static void assertRoundTrip(ColumnStatisticMetadata metadata)
    {
        assertThat(CODEC.fromJson(CODEC.toJson(metadata))).isEqualTo(metadata);
    }
}
