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

import io.trino.testing.MaterializedRow;
import io.trino.testing.TestingConnectorBehavior;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstObjectStoreIcebergConnectorTest
        extends BaseObjectStoreIcebergConnectorTest
{
    public TestStarburstObjectStoreIcebergConnectorTest()
    {
        super(false);
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_REFRESH_VIEW -> true;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasMessage("Hive metastore does not support renaming schemas");
    }

    @Test
    @Override
    public void testRenameSchemaToLongName()
    {
        assertThatThrownBy(super::testRenameSchemaToLongName)
                .hasMessage("Hive metastore does not support renaming schemas");
    }

    @Test
    @Override
    public void testDropSchemaCascadeFailure()
    {
        assertThatThrownBy(super::testDropSchemaCascadeFailure)
                .hasMessageContaining("test_system_table$partitions is not a valid object name");
    }

    @Test
    @Override // Override because the default schema exists in Hive metastore
    public void testIcebergTablesSystemTable()
    {
        // Avoid using exact match since other tests may create additional schemas
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet())
                .contains(
                        "information_schema",
                        "default",
                        "system",
                        "tpch");

        assertThat(computeActual("SELECT * FROM information_schema.schemata").getMaterializedRows())
                .contains(
                        new MaterializedRow(List.of("objectstore", "information_schema")),
                        new MaterializedRow(List.of("objectstore", "default")),
                        new MaterializedRow(List.of("objectstore", "system")),
                        new MaterializedRow(List.of("objectstore", "tpch")));

        assertThat(computeActual("SHOW TABLES FROM system").getOnlyColumnAsSet())
                .containsExactlyInAnyOrder("iceberg_tables");

        assertQuery("SELECT * FROM information_schema.tables WHERE table_schema = 'system'",
                "VALUES ('objectstore', 'system', 'iceberg_tables', 'BASE TABLE')");
    }

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyRefreshMaterializedViewFailureWithoutMultiWriteInTransactionSupport(AbstractThrowableAssert abstractThrowableAssert)
    {
        abstractThrowableAssert.hasMessageMatching("Catalog only supports writes using autocommit: \\w+");
    }

    @Override
    protected void verifyConcurrentAddColumnFailurePermissible(Exception e)
    {
        assertThat(e)
                .hasMessageMatching("Failed to add column: Metadata location .* is not same as table metadata location .* for .*");
    }
}
