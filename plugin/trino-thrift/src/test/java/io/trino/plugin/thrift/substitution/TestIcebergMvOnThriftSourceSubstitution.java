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
package io.trino.plugin.thrift.substitution;

import io.trino.plugin.iceberg.substitution.AbstractIcebergReadOnlyMvSubstitutionTest;
import io.trino.plugin.thrift.integration.ThriftQueryRunner;
import io.trino.plugin.thrift.integration.ThriftQueryRunner.StartedServers;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;

public class TestIcebergMvOnThriftSourceSubstitution
        extends AbstractIcebergReadOnlyMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner()
            throws Exception
    {
        StartedServers servers = ThriftQueryRunner.startThriftServers(1, false);
        servers.servers().forEach(server -> closeAfterClass(server::shutdown));
        servers.resources().forEach(this::closeAfterClass);
        return ThriftQueryRunner.builder(servers)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    @Override
    protected CatalogSchemaTableName substitutedTable()
    {
        return new CatalogSchemaTableName("thrift", "tiny", "nation");
    }

    @Override
    protected CatalogSchemaTableName otherTable()
    {
        return new CatalogSchemaTableName("thrift", "tiny", "region");
    }

    @Override
    protected String materializedColumn()
    {
        return "nationkey";
    }

    @Override
    protected String nonMaterializedColumn()
    {
        return "comment";
    }
}
