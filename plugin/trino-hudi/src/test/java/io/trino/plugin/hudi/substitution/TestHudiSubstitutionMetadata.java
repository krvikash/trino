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
package io.trino.plugin.hudi.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveColumnProjectionInfo;
import io.trino.plugin.hive.substitution.HiveColumnId;
import io.trino.plugin.hive.substitution.HiveTableId;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiSubstitutionMetadata
{
    private static final HudiSubstitutionMetadata METADATA = new HudiSubstitutionMetadata();
    private static final HiveColumnHandle ORDERKEY = createBaseColumn("orderkey", 0, HIVE_LONG, BIGINT, REGULAR, Optional.empty());
    private static final String SCHEMA_NAME = "s";
    private static final String TABLE_NAME = "t";
    private static final String BASE_PATH = "local:///s/t";

    @Test
    public void testGetTableIdForPlainScan()
    {
        Optional<ConnectorTableId> tableId = METADATA.getTableId(SESSION, plainHandle());

        assertThat(tableId).contains(new HudiTableId(SCHEMA_NAME, TABLE_NAME, BASE_PATH));
    }

    @Test
    public void testGetTableIdEmptyWhenPartitionPredicatePushedDown()
    {
        HudiTableHandle handle = tableHandle(ImmutableSet.of(), predicateOnOrderkey(), TupleDomain.all());

        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @Test
    public void testGetTableIdEmptyWhenRegularPredicatePushedDown()
    {
        HudiTableHandle handle = tableHandle(ImmutableSet.of(), TupleDomain.all(), predicateOnOrderkey());

        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @Test
    public void testGetTableIdEmptyWhenConstraintColumnsRecorded()
    {
        // A constraint column is recorded even for a predicate the connector does not enforce, so the
        // handle no longer represents the full source table.
        HudiTableHandle handle = tableHandle(ImmutableSet.of(ORDERKEY), TupleDomain.all(), TupleDomain.all());

        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @Test
    public void testTableHandleMatchesId()
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new HudiTableId(SCHEMA_NAME, TABLE_NAME, BASE_PATH))).isTrue();
    }

    @Test
    public void testTableHandleDoesNotMatchDifferentTable()
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new HudiTableId("other", TABLE_NAME, BASE_PATH))).isFalse();
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new HudiTableId(SCHEMA_NAME, "other", BASE_PATH))).isFalse();
    }

    @Test
    public void testTableHandleDoesNotMatchDifferentBasePath()
    {
        // Same name, different data: a table re-registered against another location must not match a
        // materialization indexed for the previous one.
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new HudiTableId(SCHEMA_NAME, TABLE_NAME, "local:///s/t_old"))).isFalse();
    }

    @Test
    public void testTableHandleDoesNotMatchOtherConnectorId()
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new HiveTableId(SCHEMA_NAME, TABLE_NAME))).isFalse();
    }

    @Test
    public void testTableHandleDoesNotMatchWhenPredicatePushedDown()
    {
        HudiTableHandle handle = tableHandle(ImmutableSet.of(), predicateOnOrderkey(), TupleDomain.all());

        assertThat(METADATA.tableHandleMatchesId(SESSION, handle, new HudiTableId(SCHEMA_NAME, TABLE_NAME, BASE_PATH))).isFalse();
    }

    @Test
    public void testGetColumnIdForBaseColumn()
    {
        // Hudi columns are Hive column handles, so they carry the Hive column identity - the delegating
        // connectors route them to the Hive delegate and must get the same id.
        Optional<ConnectorColumnId> columnId = METADATA.getColumnId(SESSION, ORDERKEY);

        assertThat(columnId).contains(new HiveColumnId("orderkey", ImmutableList.of()));
    }

    @Test
    public void testGetColumnIdKeepsSubFieldsDistinct()
    {
        // Hudi does not push dereferences into the scan today, but the id must still tell two
        // same-typed sub-fields of one column apart if it ever does.
        ConnectorColumnId a = METADATA.getColumnId(SESSION, subField("a", 0)).orElseThrow();
        ConnectorColumnId b = METADATA.getColumnId(SESSION, subField("b", 1)).orElseThrow();

        assertThat(a).isEqualTo(new HiveColumnId("info", ImmutableList.of("a")));
        assertThat(a).isNotEqualTo(b);
    }

    @Test
    public void testIdVersions()
    {
        assertThat(METADATA.tableIdVersions()).containsExactly(HudiTableId.VERSION);
        assertThat(METADATA.columnIdVersions()).containsExactly(HiveColumnId.VERSION);
    }

    private static HudiTableHandle plainHandle()
    {
        return tableHandle(ImmutableSet.of(), TupleDomain.all(), TupleDomain.all());
    }

    private static HudiTableHandle tableHandle(
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates)
    {
        // If a new field is added to the HudiTableHandle,
        // add it to the `HudiSubstitutionMetadata.substitutionCandidate` if it is a result of a pushdown,
        // or add it to the DeltaLakeTableId if it influences the table identity.
        return new HudiTableHandle(
                SCHEMA_NAME,
                TABLE_NAME,
                BASE_PATH,
                COPY_ON_WRITE,
                ImmutableList.of(),
                constraintColumns,
                partitionPredicates,
                regularPredicates);
    }

    private static TupleDomain<HiveColumnHandle> predicateOnOrderkey()
    {
        return TupleDomain.withColumnDomains(ImmutableMap.of(ORDERKEY, Domain.singleValue(BIGINT, 1L)));
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
}
