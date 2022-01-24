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

import org.junit.jupiter.api.Test;

import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestStarburstObjectStoreHudiConnectorTest
        extends BaseObjectStoreHudiConnectorTest
{
    public TestStarburstObjectStoreHudiConnectorTest()
    {
        super(false);
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

    @Override
    protected OptionalInt maxSchemaNameLength()
    {
        return OptionalInt.of(128);
    }
}
