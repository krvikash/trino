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
package io.trino.plugin.hive.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveSubstitutionMetadata
{
    private static final HiveSubstitutionMetadata METADATA = new HiveSubstitutionMetadata();
    private static final HiveColumnHandle ORDERKEY = createBaseColumn("orderkey", 0, HIVE_LONG, BIGINT, REGULAR, Optional.empty());

    @Test
    public void testGetTableIdForPlainScan()
    {
        Optional<ConnectorTableId> tableId = METADATA.getTableId(SESSION, plainHandle());

        assertThat(tableId).contains(new HiveTableId("s", "t"));
    }

    @Test
    public void testGetTableIdEmptyWhenPredicateEnforced()
    {
        assertThat(METADATA.getTableId(SESSION, handleWithEnforcedPredicate())).isEmpty();
    }

    @Test
    public void testTableHandleMatchesId()
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new HiveTableId("s", "t"))).isTrue();
    }

    @Test
    public void testTableHandleDoesNotMatchDifferentTable()
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new HiveTableId("s", "other"))).isFalse();
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new HiveTableId("other", "t"))).isFalse();
    }

    @Test
    public void testTableHandleDoesNotMatchWhenPredicateEnforced()
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, handleWithEnforcedPredicate(), new HiveTableId("s", "t"))).isFalse();
    }

    @Test
    public void testGetColumnIdForBaseColumn()
    {
        Optional<ConnectorColumnId> columnId = METADATA.getColumnId(SESSION, ORDERKEY);

        assertThat(columnId).contains(new HiveColumnId("orderkey", ImmutableList.of()));
    }

    @Test
    public void testGetColumnIdForProjectedSubField()
    {
        Optional<ConnectorColumnId> columnId = METADATA.getColumnId(SESSION, subField("a", 0));

        assertThat(columnId).contains(new HiveColumnId("info", ImmutableList.of("a")));
    }

    @Test
    public void testDifferentSubFieldsWithSameTypeHaveDifferentColumnId()
    {
        // Two sub-fields of the same struct with the same type must not collapse to one id, otherwise
        // substitution could serve info.b from a materialization of info.a.
        ConnectorColumnId a = METADATA.getColumnId(SESSION, subField("a", 0)).orElseThrow();
        ConnectorColumnId b = METADATA.getColumnId(SESSION, subField("b", 1)).orElseThrow();

        assertThat(a).isNotEqualTo(b);
    }

    // A projection of struct column info ROW(a VARCHAR, b VARCHAR) onto the given field.
    private static HiveColumnHandle subField(String field, int index)
    {
        RowType rowType = RowType.rowType(RowType.field("a", VARCHAR), RowType.field("b", VARCHAR));
        HiveColumnProjectionInfo projection = new HiveColumnProjectionInfo(
                ImmutableList.of(index),
                ImmutableList.of(field),
                toHiveType(VARCHAR),
                VARCHAR);
        return new HiveColumnHandle("info", 1, toHiveType(rowType), rowType, Optional.of(projection), REGULAR, Optional.empty());
    }

    private static HiveTableHandle plainHandle()
    {
        return new HiveTableHandle(
                "s",
                "t",
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableList.of(ORDERKEY),
                Optional.empty());
    }

    private static HiveTableHandle handleWithEnforcedPredicate()
    {
        return new HiveTableHandle(
                "s",
                "t",
                ImmutableList.of(),
                ImmutableList.of(ORDERKEY),
                TupleDomain.all(),
                TupleDomain.withColumnDomains(ImmutableMap.<ColumnHandle, Domain>of(ORDERKEY, Domain.singleValue(BIGINT, 1L))),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                NO_ACID_TRANSACTION);
    }
}
