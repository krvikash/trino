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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;

public class TestNameBasedColumnMatcher
{
    @Test
    public void testFindColumnCaseInsensitive()
    {
        NameBasedColumnMatcher matcher = new NameBasedColumnMatcher();

        MessageType schema = new MessageType("test",
                Types.primitive(BINARY, REQUIRED).named("name"),
                Types.primitive(INT32, OPTIONAL).named("age"),
                Types.primitive(BINARY, OPTIONAL).named("City"));

        // Exact match
        HiveColumnHandle nameColumn = createColumn("name");
        assertThat(matcher.findColumn(nameColumn, schema))
                .isPresent()
                .hasValueSatisfying(type -> assertThat(type.getName()).isEqualTo("name"));

        // Case insensitive match - lowercase
        HiveColumnHandle cityColumn = createColumn("city");
        assertThat(matcher.findColumn(cityColumn, schema))
                .isPresent()
                .hasValueSatisfying(type -> assertThat(type.getName()).isEqualTo("City"));

        // Case insensitive match - uppercase
        HiveColumnHandle ageColumn = createColumn("AGE");
        assertThat(matcher.findColumn(ageColumn, schema))
                .isPresent()
                .hasValueSatisfying(type -> assertThat(type.getName()).isEqualTo("age"));

        // Column not found
        HiveColumnHandle missingColumn = createColumn("missing");
        assertThat(matcher.findColumn(missingColumn, schema)).isEmpty();
    }

    @Test
    public void testClipSchema()
    {
        NameBasedColumnMatcher matcher = new NameBasedColumnMatcher();

        MessageType fullSchema = new MessageType("test",
                Types.primitive(BINARY, REQUIRED).named("col1"),
                Types.primitive(INT32, OPTIONAL).named("col2"),
                Types.primitive(BINARY, OPTIONAL).named("col3"),
                Types.primitive(INT32, OPTIONAL).named("col4"));

        // Request subset of columns with different cases
        HiveColumnHandle col2 = createColumn("COL2");
        HiveColumnHandle col4 = createColumn("col4");

        MessageType clippedSchema = matcher.clipSchema(fullSchema, ImmutableList.of(col2, col4));

        assertThat(clippedSchema.getFields()).hasSize(2);
        assertThat(clippedSchema.getFields().get(0).getName()).isEqualTo("col2");
        assertThat(clippedSchema.getFields().get(1).getName()).isEqualTo("col4");
    }

    @Test
    public void testClipSchemaWithMissingColumns()
    {
        NameBasedColumnMatcher matcher = new NameBasedColumnMatcher();

        MessageType fullSchema = new MessageType("test",
                Types.primitive(BINARY, REQUIRED).named("col1"),
                Types.primitive(INT32, OPTIONAL).named("col2"));

        HiveColumnHandle existingColumn = createColumn("col1");
        HiveColumnHandle missingColumn = createColumn("col999");

        MessageType clippedSchema = matcher.clipSchema(fullSchema, ImmutableList.of(existingColumn, missingColumn));

        // Only existing column should be in clipped schema
        assertThat(clippedSchema.getFields()).hasSize(1);
        assertThat(clippedSchema.getFields().get(0).getName()).isEqualTo("col1");
    }

    @Test
    public void testClipSchemaEmpty()
    {
        NameBasedColumnMatcher matcher = new NameBasedColumnMatcher();

        MessageType fullSchema = new MessageType("test",
                Types.primitive(BINARY, REQUIRED).named("col1"),
                Types.primitive(INT32, OPTIONAL).named("col2"));

        MessageType clippedSchema = matcher.clipSchema(fullSchema, ImmutableList.of());

        assertThat(clippedSchema.getFields()).isEmpty();
    }

    @Test
    public void testClipSchemaPreservesOrder()
    {
        NameBasedColumnMatcher matcher = new NameBasedColumnMatcher();

        MessageType fullSchema = new MessageType("test",
                Types.primitive(BINARY, REQUIRED).named("a"),
                Types.primitive(INT32, OPTIONAL).named("b"),
                Types.primitive(BINARY, OPTIONAL).named("c"),
                Types.primitive(INT32, OPTIONAL).named("d"));

        // Request in different order than schema
        MessageType clippedSchema = matcher.clipSchema(fullSchema, ImmutableList.of(
                createColumn("d"),
                createColumn("b"),
                createColumn("a")));

        // Should preserve the order in which columns were requested
        assertThat(clippedSchema.getFields()).hasSize(3);
        assertThat(clippedSchema.getFields().get(0).getName()).isEqualTo("d");
        assertThat(clippedSchema.getFields().get(1).getName()).isEqualTo("b");
        assertThat(clippedSchema.getFields().get(2).getName()).isEqualTo("a");
    }

    private HiveColumnHandle createColumn(String name)
    {
        return new HiveColumnHandle(
                name,
                0,
                HiveType.HIVE_STRING,
                VarcharType.VARCHAR,
                Optional.empty(),
                REGULAR,
                Optional.empty());
    }
}
