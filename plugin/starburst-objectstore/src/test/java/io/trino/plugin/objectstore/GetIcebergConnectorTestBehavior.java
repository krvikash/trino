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

import io.trino.plugin.iceberg.BaseIcebergConnectorTest;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.testing.TestingConnectorBehavior;

import java.util.Optional;

class GetIcebergConnectorTestBehavior
        extends BaseIcebergConnectorTest
{
    GetIcebergConnectorTestBehavior()
    {
        super(new IcebergConfig().getFileFormat());
    }

    @Override
    protected boolean isFileSorted(String path, String sortColumnName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        throw new UnsupportedOperationException();
    }

    // Override to make visible
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return super.hasBehavior(connectorBehavior);
    }

    // Override to make visible
    @Override
    protected Optional<SetColumnTypeSetup> filterSetFieldTypesDataProvider(SetColumnTypeSetup setup)
    {
        return super.filterSetFieldTypesDataProvider(setup);
    }

    // Override to make visible
    @Override
    protected void verifySetFieldTypeFailurePermissible(Throwable e)
    {
        super.verifySetFieldTypeFailurePermissible(e);
    }
}
