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
package io.trino.plugin.deltalake.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.plugin.deltalake.DeltaLakeColumnProjectionInfo;
import io.trino.plugin.deltalake.DeltaLakeTableHandle;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.hive.substitution.HiveTableId;
import io.trino.spi.connector.substitution.ConnectorColumnId;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.RowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.stream.Stream;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

public class TestDeltaLakeSubstitutionMetadata
{
    private static final DeltaLakeSubstitutionMetadata METADATA = new DeltaLakeSubstitutionMetadata();

    private static final String DELTA_TABLE_ID = "40ad50cf-73ec-4e77-a2ee-ee66b4b8c9d0";
    public static final String SCHEMA = "schema";
    public static final String TABLE = "table";
    private static final DeltaLakeTableId TABLE_ID = new DeltaLakeTableId(SCHEMA, TABLE, DELTA_TABLE_ID);
    private static final MetadataEntry METADATA_ENTRY = metadataEntry(DELTA_TABLE_ID);
    private static final ProtocolEntry PROTOCOL_ENTRY = new ProtocolEntry(3, 7, Optional.empty(), Optional.empty());

    // Column mapping mode id or name gives a column a physical name unrelated to its logical name.
    private static final DeltaLakeColumnHandle ORDERKEY = new DeltaLakeColumnHandle(
            "orderkey",
            BIGINT,
            OptionalInt.empty(),
            "col-6707cc9e-f3aa-4e6b-b8ef-1b03d3475680",
            BIGINT,
            REGULAR,
            Optional.empty());

    @Test
    public void testGetTableIdForPlainScan()
    {
        Optional<ConnectorTableId> tableId = METADATA.getTableId(SESSION, plainHandle());

        assertThat(tableId).contains(TABLE_ID);
    }

    @Test
    public void testGetTableIdIgnoresProjectedColumns()
    {
        // Projected columns do not affect table identity, and they are populated only in the fully optimized plan.
        DeltaLakeTableHandle handle = plainHandle().withProjectedColumns(ImmutableSet.of(ORDERKEY));

        assertThat(METADATA.getTableId(SESSION, handle)).contains(TABLE_ID);
    }

    @Test
    public void testGetTableIdEmptyWithoutDeltaTableId()
    {
        // The transaction log is not required to carry a table id; without it there is no stable identity.
        DeltaLakeTableHandle handle = handle(
                metadataEntry(null),
                TupleDomain.all(),
                TupleDomain.all(),
                ImmutableSet.of(),
                false);

        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonPlainHandles")
    public void testGetTableIdEmptyForNonPlainScan(String description, DeltaLakeTableHandle handle)
    {
        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("nonPlainHandles")
    public void testTableHandleDoesNotMatchIdForNonPlainScan(String description, DeltaLakeTableHandle handle)
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, handle, TABLE_ID)).isFalse();
    }

    @Test
    public void testTableHandleMatchesId()
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), TABLE_ID)).isTrue();
    }

    @Test
    public void testTableHandleDoesNotMatchDifferentTable()
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new DeltaLakeTableId("other", TABLE, DELTA_TABLE_ID))).isFalse();
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new DeltaLakeTableId(SCHEMA, "other", DELTA_TABLE_ID))).isFalse();
    }

    @Test
    public void testTableHandleDoesNotMatchRecreatedTable()
    {
        // Same name, different transaction log id: the table was dropped and recreated, so a materialization
        // indexed for the previous table must not be substituted.
        ConnectorTableId recreated = new DeltaLakeTableId(SCHEMA, TABLE, "some-other-id");

        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), recreated)).isFalse();
    }

    @Test
    public void testTableHandleDoesNotMatchTableIdOfAnotherConnector()
    {
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), new HiveTableId(SCHEMA, TABLE))).isFalse();
    }

    @Test
    public void testGetColumnIdForBaseColumn()
    {
        Optional<ConnectorColumnId> columnId = METADATA.getColumnId(SESSION, ORDERKEY);

        assertThat(columnId).contains(new DeltaLakeColumnId(ORDERKEY.basePhysicalColumnName(), ImmutableList.of()));
    }

    @Test
    public void testGetColumnIdForProjectedSubField()
    {
        Optional<ConnectorColumnId> columnId = METADATA.getColumnId(SESSION, subField("col-a", 0));

        assertThat(columnId).contains(new DeltaLakeColumnId("col-info", ImmutableList.of("col-a")));
    }

    @Test
    public void testDifferentSubFieldsWithSameTypeHaveDifferentColumnId()
    {
        // Two sub-fields of the same struct with the same type must not collapse to one id, otherwise
        // substitution could serve info.b from a materialization of info.a.
        ConnectorColumnId a = METADATA.getColumnId(SESSION, subField("col-a", 0)).orElseThrow();
        ConnectorColumnId b = METADATA.getColumnId(SESSION, subField("col-b", 1)).orElseThrow();

        assertThat(a).isNotEqualTo(b);
    }

    @Test
    public void testSubFieldHasDifferentColumnIdThanBaseColumn()
    {
        ConnectorColumnId baseColumn = METADATA.getColumnId(SESSION, baseInfoColumn()).orElseThrow();
        ConnectorColumnId subField = METADATA.getColumnId(SESSION, subField("col-a", 0)).orElseThrow();

        assertThat(baseColumn).isNotEqualTo(subField);
    }

    private static Stream<Arguments> nonPlainHandles()
    {
        return Stream.of(
                arguments(
                        "enforced partition constraint",
                        handle(
                                METADATA_ENTRY,
                                TupleDomain.withColumnDomains(ImmutableMap.of(ORDERKEY, Domain.singleValue(BIGINT, 1L))),
                                TupleDomain.all(),
                                ImmutableSet.of(),
                                false)),
                arguments(
                        "non-partition constraint",
                        handle(
                                METADATA_ENTRY,
                                TupleDomain.all(),
                                TupleDomain.withColumnDomains(ImmutableMap.of(ORDERKEY, Domain.singleValue(BIGINT, 1L))),
                                ImmutableSet.of(),
                                false)),
                arguments(
                        "constraint columns",
                        handle(
                                METADATA_ENTRY,
                                TupleDomain.all(),
                                TupleDomain.all(),
                                ImmutableSet.of(ORDERKEY),
                                false)),
                arguments(
                        "time travel",
                        handle(
                                METADATA_ENTRY,
                                TupleDomain.all(),
                                TupleDomain.all(),
                                ImmutableSet.of(),
                                true)),
                arguments("merge", plainHandle().forMerge()),
                arguments("optimize", plainHandle().forOptimize(true, DataSize.of(1, MEGABYTE))));
    }

    private static DeltaLakeTableHandle plainHandle()
    {
        return handle(METADATA_ENTRY, TupleDomain.all(), TupleDomain.all(), ImmutableSet.of(), false);
    }

    private static DeltaLakeTableHandle handle(
            MetadataEntry metadataEntry,
            TupleDomain<DeltaLakeColumnHandle> enforcedPartitionConstraint,
            TupleDomain<DeltaLakeColumnHandle> nonPartitionConstraint,
            Set<DeltaLakeColumnHandle> constraintColumns,
            boolean timeTravel)
    {
        // If a new field is added to the DeltaLakeTableHandle,
        // add it to the `DeltaLakeSubstitutionMetadata.substitutionCandidate` if it is a result of a pushdown,
        // or add it to the DeltaLakeTableId if it influences the table identity.
        return new DeltaLakeTableHandle(
                SCHEMA,
                TABLE,
                true,
                false,
                Optional.empty(),
                "location",
                metadataEntry,
                PROTOCOL_ENTRY,
                enforcedPartitionConstraint,
                nonPartitionConstraint,
                constraintColumns,
                false,
                Optional.empty(),
                Optional.empty(),
                false,
                false,
                Optional.empty(),
                1,
                timeTravel);
    }

    private static MetadataEntry metadataEntry(String id)
    {
        return new MetadataEntry(
                id,
                "name",
                "description",
                new MetadataEntry.Format("provider", ImmutableMap.of()),
                "{\"fields\": [{\"name\": \"orderkey\", \"metadata\": {}}]}",
                ImmutableList.of(),
                ImmutableMap.of(),
                0);
    }

    // A projection of struct column info ROW(a VARCHAR, b VARCHAR) onto the given field, referenced by physical name.
    private static DeltaLakeColumnHandle subField(String physicalFieldName, int index)
    {
        DeltaLakeColumnProjectionInfo projection = new DeltaLakeColumnProjectionInfo(
                VARCHAR,
                ImmutableList.of(index),
                ImmutableList.of(physicalFieldName));
        return new DeltaLakeColumnHandle(
                "info",
                infoType(),
                OptionalInt.empty(),
                "col-info",
                physicalInfoType(),
                REGULAR,
                Optional.of(projection));
    }

    private static DeltaLakeColumnHandle baseInfoColumn()
    {
        return new DeltaLakeColumnHandle("info", infoType(), OptionalInt.empty(), "col-info", physicalInfoType(), REGULAR, Optional.empty());
    }

    private static RowType infoType()
    {
        return RowType.rowType(RowType.field("a", VARCHAR), RowType.field("b", VARCHAR));
    }

    private static RowType physicalInfoType()
    {
        return RowType.rowType(RowType.field("col-a", VARCHAR), RowType.field("col-b", VARCHAR));
    }
}
