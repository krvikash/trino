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

import io.trino.plugin.hudi.HudiConnector;
import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;

import java.util.List;

import static io.trino.testing.TransactionBuilder.transaction;

public class TpchObjectStoreHudiTablesInitializer
        extends TpchHudiTablesInitializer
{
    public TpchObjectStoreHudiTablesInitializer(List<TpchTable<?>> tpchTables)
    {
        super(tpchTables);
    }

    @Override
    protected HudiConnector getHudiConnector(QueryRunner queryRunner)
    {
        ObjectStoreConnector objectStoreConnector = transaction(queryRunner.getTransactionManager(), queryRunner.getPlannerContext().getMetadata(), queryRunner.getAccessControl())
                .execute(queryRunner.getDefaultSession(), transactionSession -> (ObjectStoreConnector) queryRunner.getCoordinator().getConnector(transactionSession, "objectstore"));
        return (HudiConnector) objectStoreConnector.getInjector().getInstance(DelegateConnectors.class).hudiConnector();
    }
}
