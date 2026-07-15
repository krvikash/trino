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
package io.trino.plugin.bigquery.substitution;

import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.plugin.bigquery.BigQueryColumnHandle;
import io.trino.plugin.bigquery.BigQueryNamedRelationHandle;
import io.trino.plugin.bigquery.BigQueryQueryRelationHandle;
import io.trino.plugin.bigquery.BigQueryTableHandle;
import io.trino.plugin.bigquery.RemoteTableName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.substitution.ConnectorTableId;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBigQuerySubstitutionMetadata
{
    private static final BigQuerySubstitutionMetadata METADATA = new BigQuerySubstitutionMetadata();

    private static final BigQueryNamedRelationHandle NAMED_RELATION = new BigQueryNamedRelationHandle(
            new SchemaTableName("tpch", "orders"),
            new RemoteTableName("test-project", "tpch", "orders"),
            "TABLE",
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            false);

    private static final BigQueryColumnHandle COLUMN = new BigQueryColumnHandle(
            "orderkey",
            ImmutableList.of(),
            BIGINT,
            StandardSQLTypeName.INT64,
            true,
            Field.Mode.NULLABLE,
            ImmutableList.of(),
            null,
            false);

    private static BigQueryTableHandle plainHandle()
    {
        // Mirror BigQueryMetadata.getTableHandle, which always populates projectedColumns with all
        // columns. A handle in this shape must still be a substitution candidate.
        return new BigQueryTableHandle(NAMED_RELATION, TupleDomain.all(), Optional.of(ImmutableList.of(COLUMN)), OptionalLong.empty());
    }

    @Test
    public void testGetTableIdForPlainTable()
    {
        assertThat(METADATA.getTableId(SESSION, plainHandle()))
                .contains(new BigQueryTableId(NAMED_RELATION.getRemoteTableName()));
    }

    @Test
    public void testGetTableIdReturnsEmptyForConstraintPushdown()
    {
        BigQueryTableHandle handle = new BigQueryTableHandle(
                NAMED_RELATION,
                TupleDomain.withColumnDomains(ImmutableMap.of(COLUMN, Domain.singleValue(BIGINT, 1L))),
                Optional.of(ImmutableList.of(COLUMN)),
                OptionalLong.empty());
        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @Test
    public void testGetTableIdReturnsEmptyForLimitPushdown()
    {
        BigQueryTableHandle handle = plainHandle().withLimit(10);
        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @Test
    public void testGetTableIdReturnsEmptyForSyntheticTable()
    {
        BigQueryQueryRelationHandle queryRelation = new BigQueryQueryRelationHandle(
                "SELECT 1",
                new RemoteTableName("test-project", "tpch", "_destination"),
                false);
        BigQueryTableHandle handle = new BigQueryTableHandle(queryRelation, TupleDomain.all(), Optional.empty(), OptionalLong.empty());
        assertThat(METADATA.getTableId(SESSION, handle)).isEmpty();
    }

    @Test
    public void testTableHandleMatchesId()
    {
        ConnectorTableId id = new BigQueryTableId(NAMED_RELATION.getRemoteTableName());

        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), id)).isTrue();

        BigQueryTableId otherTableId = new BigQueryTableId(new RemoteTableName("test-project", "tpch", "lineitem"));
        assertThat(METADATA.tableHandleMatchesId(SESSION, plainHandle(), otherTableId)).isFalse();

        // handle carrying a pushdown is not a substitution candidate
        BigQueryTableHandle withLimit = plainHandle().withLimit(10);
        assertThat(METADATA.tableHandleMatchesId(SESSION, withLimit, id)).isFalse();
    }

    @Test
    public void testGetColumnId()
    {
        assertThat(METADATA.getColumnId(SESSION, COLUMN))
                .contains(new BigQueryColumnId("orderkey", ImmutableList.of(), BIGINT, StandardSQLTypeName.INT64));
    }

    @Test
    public void testTableIdJsonRoundTrip()
    {
        JsonCodec<BigQueryTableId> codec = new JsonCodecFactory().jsonCodec(BigQueryTableId.class);
        BigQueryTableId expected = new BigQueryTableId(NAMED_RELATION.getRemoteTableName());
        assertThat(codec.fromJson(codec.toJson(expected))).isEqualTo(expected);
    }
}
