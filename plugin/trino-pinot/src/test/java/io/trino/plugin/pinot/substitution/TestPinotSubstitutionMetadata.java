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
package io.trino.plugin.pinot.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotTableHandle;
import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPinotSubstitutionMetadata
{
    private static final PinotSubstitutionMetadata METADATA = new PinotSubstitutionMetadata();

    private static final PinotColumnHandle COLUMN = new PinotColumnHandle("orderkey", BIGINT);

    private static PinotTableHandle plainHandle()
    {
        return new PinotTableHandle("tpch", "orders", false, TupleDomain.all(), OptionalLong.empty(), Optional.empty());
    }

    @Test
    public void testGetTableIdForPlainTable()
    {
        assertThat(METADATA.getTableId(SESSION, plainHandle()))
                .contains(new PinotTableId("tpch", "orders"));
    }

    @Test
    public void testGetTableIdIgnoresEnableNullHandling()
    {
        PinotTableHandle handle = new PinotTableHandle("tpch", "orders", true, TupleDomain.all(), OptionalLong.empty(), Optional.empty());
        assertThat(METADATA.getTableId(SESSION, handle))
                .contains(new PinotTableId("tpch", "orders"));
    }

    @Test
    public void testGetTableIdReturnsEmptyForConstraintPushdown()
    {
        TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN, Domain.singleValue(BIGINT, 1L)));
        PinotTableHandle handle = new PinotTableHandle("tpch", "orders", false, constraint, OptionalLong.empty(), Optional.empty());
        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @Test
    public void testGetTableIdReturnsEmptyForLimitPushdown()
    {
        PinotTableHandle handle = new PinotTableHandle("tpch", "orders", false, TupleDomain.all(), OptionalLong.of(10), Optional.empty());
        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @Test
    public void testGetTableIdReturnsEmptyForDynamicTable()
    {
        PinotTableHandle handle = new PinotTableHandle("tpch", "orders", false, TupleDomain.all(), OptionalLong.empty(), Optional.of(dynamicTable()));
        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @Test
    public void testTableHandleMatchesId()
    {
        PinotTableId id = new PinotTableId("tpch", "orders");

        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), id)).isTrue();
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new PinotTableId("tpch", "lineitem"))).isFalse();
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new PinotTableId("other", "orders"))).isFalse();

        // handle carrying a pushdown is not a substitution candidate
        PinotTableHandle withLimit = new PinotTableHandle("tpch", "orders", false, TupleDomain.all(), OptionalLong.of(10), Optional.empty());
        assertThat(METADATA.tableHandleMatchesId(SESSION, withLimit, id)).isFalse();
    }

    @Test
    public void testGetColumnId()
    {
        assertThat(METADATA.getColumnId(SESSION, COLUMN))
                .contains(new PinotColumnId("orderkey"));
    }

    @Test
    public void testColumnIdIgnoresReadPathAttributes()
    {
        PinotColumnHandle aliasedAggregate = new PinotColumnHandle(
                "orderkey",
                BIGINT,
                "max(orderkey)",
                true,
                true,
                true,
                Optional.of("max"),
                Optional.of("orderkey"));
        assertThat(METADATA.getColumnId(SESSION, aliasedAggregate))
                .contains(new PinotColumnId("orderkey"));
    }

    @Test
    public void testTableIdHashDistinguishesSchemaFromTable()
    {
        // the schema/table boundary is part of the hashed identity: "a"."b:c" is not "a:b"."c"
        assertThat(new PinotTableId("a", "b:c").hash())
                .isNotEqualTo(new PinotTableId("a:b", "c").hash());
    }

    @Test
    public void testTableIdJsonRoundTrip()
    {
        JsonCodec<PinotTableId> codec = new JsonCodecFactory().jsonCodec(PinotTableId.class);
        PinotTableId expected = new PinotTableId("tpch", "orders");
        assertThat(codec.fromJson(codec.toJson(expected))).isEqualTo(expected);
    }

    private static DynamicTable dynamicTable()
    {
        return new DynamicTable(
                "orders",
                Optional.empty(),
                ImmutableList.of(COLUMN),
                Optional.empty(),
                ImmutableList.of(),
                ImmutableList.of(),
                Optional.empty(),
                ImmutableList.of(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                ImmutableMap.of(),
                "SELECT orderkey FROM orders");
    }
}
