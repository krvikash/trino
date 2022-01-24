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
package io.trino.plugin.objectstore;

import io.trino.spi.connector.SchemaTableName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.plugin.objectstore.RelationType.DELTA_TABLE;
import static io.trino.plugin.objectstore.RelationType.MATERIALIZED_VIEW;
import static io.trino.plugin.objectstore.RelationType.VIEW;
import static io.trino.plugin.objectstore.TableType.DELTA;
import static io.trino.plugin.objectstore.TableType.ICEBERG;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRelationTypeCache
{
    @Test
    public void testDefaultToIceberg()
    {
        assertAffinity(new RelationTypeCache(), new SchemaTableName("some_schema", "table_name"), ICEBERG);
    }

    @Test
    public void testDefaultToMostFrequent()
    {
        for (TableType tableType : TableType.values()) {
            TableType otherType = Arrays.stream(TableType.values())
                    .filter(other -> other != tableType)
                    .findFirst().orElseThrow();

            RelationTypeCache relationTypeCache = new RelationTypeCache();
            for (int i = 0; i < 5; i++) {
                relationTypeCache.record(new SchemaTableName("some_schema", "table_name" + i), tableType);
            }

            // it should not default to recently used type
            assertAffinity(relationTypeCache, new SchemaTableName("some_schema", "other_table"), tableType);

            // one table of a different type
            relationTypeCache.record(new SchemaTableName("some_schema", "different_type"), otherType);
            assertAffinity(relationTypeCache, new SchemaTableName("some_schema", "different_type"), otherType);
            assertAffinity(relationTypeCache, new SchemaTableName("some_schema", "yet_another_table"), tableType);
        }
    }

    @Test
    public void testView()
    {
        RelationTypeCache relationTypeCache = new RelationTypeCache();
        SchemaTableName viewName = new SchemaTableName("some_schema", "view_name");

        relationTypeCache.record(viewName, VIEW);
        assertThat(relationTypeCache.getRelationType(viewName)).isEqualTo(Optional.of(VIEW));
        assertAffinity(relationTypeCache, viewName, ICEBERG);

        relationTypeCache.record(viewName, DELTA);
        assertThat(relationTypeCache.getRelationType(viewName)).isEqualTo(Optional.of(DELTA_TABLE));
        assertAffinity(relationTypeCache, viewName, DELTA);
    }

    @Test
    public void testMaterializedView()
    {
        RelationTypeCache relationTypeCache = new RelationTypeCache();
        SchemaTableName viewName = new SchemaTableName("some_schema", "materialized_view_name");

        relationTypeCache.record(viewName, MATERIALIZED_VIEW);
        assertThat(relationTypeCache.getRelationType(viewName)).isEqualTo(Optional.of(MATERIALIZED_VIEW));
        assertAffinity(relationTypeCache, viewName, ICEBERG);

        relationTypeCache.record(viewName, DELTA);
        assertThat(relationTypeCache.getRelationType(viewName)).isEqualTo(Optional.of(DELTA_TABLE));
        assertAffinity(relationTypeCache, viewName, DELTA);
    }

    private void assertAffinity(RelationTypeCache relationTypeCache, SchemaTableName schemaTableName, TableType expected)
    {
        assertThat(relationTypeCache.getTableTypeAffinity(schemaTableName).get(0))
                .isEqualTo(expected);
    }
}
