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
package io.starburst.materialization.metastore.server.db;

import io.starburst.materialization.metastore.MetastoreId;
import io.starburst.materialization.metastore.server.MaterializationAccountResolver;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.Context;
import org.flywaydb.core.api.migration.JavaMigration;
import org.jdbi.v3.core.Jdbi;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Fills {@code account_id} for rows written before it existed. Java rather than SQL because the
 * mapping from metastore to account belongs to the deployment, so it comes from the bound resolver.
 * Registered as an instance, so the version is declared here instead of in a {@code V03__} class name.
 */
public class BackfillAccountIdMigration
        implements JavaMigration
{
    private final MaterializationAccountResolver accountResolver;

    public BackfillAccountIdMigration(MaterializationAccountResolver accountResolver)
    {
        this.accountResolver = requireNonNull(accountResolver, "accountResolver is null");
    }

    @Override
    public MigrationVersion getVersion()
    {
        return MigrationVersion.fromVersion("3");
    }

    @Override
    public String getDescription()
    {
        return "backfill account id";
    }

    @Override
    public Integer getChecksum()
    {
        // Java migrations are not checksummed
        return null;
    }

    @Override
    public boolean canExecuteInTransaction()
    {
        return true;
    }

    @Override
    public void migrate(Context context)
    {
        // Flyway owns the connection and the transaction around this migration
        Jdbi.create(context.getConnection()).useHandle(handle -> {
            List<String> metastoreIds = handle.createQuery("SELECT DISTINCT metastore_id FROM materializations WHERE account_id IS NULL")
                    .mapTo(String.class)
                    .list();
            for (String metastoreId : metastoreIds) {
                handle.createUpdate("UPDATE materializations SET account_id = :accountId WHERE metastore_id = :metastoreId AND account_id IS NULL")
                        .bind("accountId", accountResolver.accountId(new MetastoreId(metastoreId)).toString())
                        .bind("metastoreId", metastoreId)
                        .execute();
            }
        });
    }
}
