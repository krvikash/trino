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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.materialization.metastore.server.MaterializationDao;
import org.jdbi.v3.core.Jdbi;

import static java.util.Objects.requireNonNull;

/**
 * Provides default MaterializationDao for the materialization metastore.
 * Requires Jdbi instance, annotated with {@link ForMaterializationMetastore},
 * and {@link DatabaseType} bean available in the context.
 */
public class MaterializationMetastoreDbModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(MaterializationMetastoreFlywayMigration.class).asEagerSingleton();
    }

    @Provides
    @Singleton
    static MaterializationDao createDao(@ForMaterializationMetastore Jdbi jdbi, DatabaseType databaseType, MaterializationMetastoreFlywayMigration migration)
    {
        // Depend on the migration so the schema is migrated before the DAO (and any query through it) is created.
        requireNonNull(migration, "migration is null");
        return switch (databaseType) {
            case POSTGRESQL -> jdbi.onDemand(PostgreSqlMaterializationDao.class);
            case MYSQL -> jdbi.onDemand(MySqlMaterializationDao.class);
            case ORACLE -> jdbi.onDemand(OracleMaterializationDao.class);
        };
    }
}
