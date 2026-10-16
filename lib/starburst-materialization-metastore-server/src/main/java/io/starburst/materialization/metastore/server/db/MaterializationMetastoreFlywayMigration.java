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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.starburst.materialization.metastore.server.MaterializationAccountResolver;
import jakarta.annotation.PostConstruct;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.migration.JavaMigration;
import org.flywaydb.core.api.output.CleanResult;
import org.flywaydb.core.api.output.MigrateResult;

import javax.sql.DataSource;

import java.util.List;
import java.util.Set;

import static org.flywaydb.core.api.MigrationVersion.LATEST;

public final class MaterializationMetastoreFlywayMigration
{
    private static final Logger log = Logger.get(MaterializationMetastoreFlywayMigration.class);
    private final Flyway flyway;

    @Inject
    public MaterializationMetastoreFlywayMigration(@ForMaterializationMetastore DataSource dataSource, DatabaseType databaseType, MaterializationAccountResolver accountResolver)
    {
        this(dataSource, ImmutableSet.of(getLocation("materialization/db", databaseType)), javaMigrations(databaseType, accountResolver), "materialization_metastore_migrations", LATEST);
    }

    private MaterializationMetastoreFlywayMigration(DataSource dataSource, Set<String> locations, List<JavaMigration> javaMigrations, String table, MigrationVersion target)
    {
        flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations(locations.toArray(String[]::new))
                .javaMigrations(javaMigrations.toArray(JavaMigration[]::new))
                .baselineOnMigrate(true)
                .baselineVersion("0")
                .target(target)
                .cleanDisabled(false)
                .table(table)
                .load();
    }

    @PostConstruct
    public void migrate()
    {
        MigrateResult migrations = flyway.migrate();
        log.info("Performed %s migrations", migrations.migrationsExecuted);
    }

    @VisibleForTesting
    public void migrate(MigrationVersion target)
    {
        Flyway.configure()
                .configuration(flyway.getConfiguration())
                .target(target)
                .load()
                .migrate();
    }

    @VisibleForTesting
    public void clean()
    {
        CleanResult result = flyway.clean();
        log.info("Dropped schemas %s, Cleaned schemas %s", result.schemasDropped, result.schemasCleaned);
    }

    private static List<JavaMigration> javaMigrations(DatabaseType databaseType, MaterializationAccountResolver accountResolver)
    {
        return switch (databaseType) {
            // SEP databases hold rows written before account ids existed
            case POSTGRESQL, MYSQL, ORACLE -> ImmutableList.of(new BackfillAccountIdMigration(accountResolver));
            // Galaxy adds account id as NOT NULL to an empty table
            case COCKROACHDB -> ImmutableList.of();
        };
    }

    private static String getLocation(String directory, DatabaseType databaseType)
    {
        if (!directory.endsWith("/")) {
            directory = directory + "/";
        }
        return switch (databaseType) {
            case POSTGRESQL -> directory + "postgresql";
            case COCKROACHDB -> directory + "cockroachdb";
            case MYSQL -> directory + "mysql";
            case ORACLE -> directory + "oracle";
        };
    }
}
