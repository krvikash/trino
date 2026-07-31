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
package io.trino.plugin.objectstore.substitution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergOnIcebergMvSubstitutionTest;
import io.trino.plugin.objectstore.ObjectStorePlugin;
import io.trino.plugin.objectstore.TableType;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Runs the Iceberg MV substitution suite against an ObjectStore catalog backed by Iceberg, exercising
 * ObjectStore's {@link ObjectStoreSubstitutionMetadata} delegation to the Iceberg connector.
 */
@Execution(SAME_THREAD)
final class TestObjectStoreIcebergMvSubstitution
        extends AbstractIcebergOnIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("objectstore");
            verify(dataDir.toFile().mkdirs());

            queryRunner.installPlugin(new ObjectStorePlugin());
            queryRunner.createCatalog(sourceSchema.getCatalogName(), STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", TableType.ICEBERG.name())
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", "local://" + dataDir)
                    .put("fs.local.enabled", "true")
                    .buildOrThrow());
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("objectstore", "schema");
    }
}
