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

import io.trino.testing.TestingConnectorBehavior;
import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.abort;

public class TestStarburstObjectStoreHiveConnectorTest
        extends BaseObjectStoreHiveConnectorTest
{
    public TestStarburstObjectStoreHiveConnectorTest()
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
        assertQueryFails("ALTER SCHEMA tpch RENAME TO tpch_renamed", "Hive metastore does not support renaming schemas");
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
    @Override
    public void testAddColumnConcurrently()
    {
        // The example failure:
        // Expecting actual:
        //   (col), (col3)
        // to contain exactly in any order:
        //   [(col), (col0), (col1), (col2), (col3)]
        // but could not find the following elements:
        //   (col0), (col1), (col2)
        abort("TODO: Enable this test after finding the failure cause");
    }

    @Test
    @Override
    public void testInsertRowConcurrently()
            throws Exception
    {
        // Hive metastore may throw an exception during concurrent inserts
        // All operations other than the following update operations were completed ... Unexpected 2 statistics for 1 columns
        abort("TODO: Enable this test after finding the failure cause");
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
    protected Optional<String> filterColumnNameTestData(String columnName)
    {
        if (columnName.equals("カラム")) {
            return Optional.empty();
        }
        return Optional.of(columnName);
    }

    @Override
    protected void verifyRefreshMaterializedViewFailureWithoutMultiWriteInTransactionSupport(AbstractThrowableAssert abstractThrowableAssert)
    {
        abstractThrowableAssert.hasMessageMatching("Catalog only supports writes using autocommit: \\w+");
    }
}
