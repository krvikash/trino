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
package io.trino.tests.substitution;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.containers.Hive3FlociDataLake;
import io.trino.plugin.hive.s3.S3HiveQueryRunner;
import io.trino.plugin.iceberg.IcebergPlugin;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.parallel.Execution;

import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.Minio.MINIO_REGION;
import static io.trino.testing.containers.Minio.MINIO_ROOT_PASSWORD;
import static io.trino.testing.containers.Minio.MINIO_ROOT_USER;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnHiveSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        String bucketName = "test-iceberg-mv-on-hive-source-" + randomNameSuffix();
        Hive3FlociDataLake hiveFlociDataLake = closeAfterClass(new Hive3FlociDataLake(bucketName));
        hiveFlociDataLake.start();

        // Hive is the source catalog (default session catalog "hive", schema "tpch"); the Iceberg
        // catalog holds the MV storage tables. Both share the dockerized Hive metastore and MinIO
        // bucket. Hive 3 gives the source tables ACID support, which the staleness tests rely on.
        QueryRunner queryRunner = S3HiveQueryRunner.builder(hiveFlociDataLake)
                .setHiveProperties(ImmutableMap.of("hive.security", "allow-all"))
                .setCreateTpchSchemas(false)
                .setSkipTimezoneSetup(true)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
        try {
            queryRunner.installPlugin(new IcebergPlugin());
            queryRunner.createCatalog(mvSchema().getCatalogName(), "iceberg", ImmutableMap.<String, String>builder()
                    .put("iceberg.catalog.type", "HIVE_METASTORE")
                    .put("hive.metastore.uri", hiveFlociDataLake.getHiveMetastoreEndpoint().toString())
                    .put("fs.hadoop.enabled", "false")
                    .put("fs.s3.enabled", "true")
                    .put("s3.aws-access-key", MINIO_ROOT_USER)
                    .put("s3.aws-secret-key", MINIO_ROOT_PASSWORD)
                    .put("s3.endpoint", hiveFlociDataLake.floci().endpoint().toString())
                    .put("s3.region", MINIO_REGION)
                    .put("s3.path-style-access", "true")
                    .buildOrThrow());

            queryRunner.execute("CREATE SCHEMA %s WITH (location = 's3a://%s/tpch')".formatted(sourceSchema, bucketName));
            queryRunner.execute("CREATE SCHEMA %s WITH (location = 's3://%s/%s')".formatted(mvSchema(), bucketName, mvSchema().getSchemaName()));
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
        return queryRunner;
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName("hive", "tpch");
    }

    @Override
    protected boolean addIcebergConnector()
    {
        return false;
    }

    @Override
    protected boolean addTpchConnector()
    {
        return false;
    }

    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }

    @Override
    protected CatalogSchemaName mvSchema()
    {
        return new CatalogSchemaName("iceberg", "iceberg_mv");
    }

    @Override
    protected String sourceTablePropertiesClause()
    {
        // Row-level DELETE/INSERT used by the staleness tests to age the MV are supported by Hive
        // only on ACID (transactional) tables.
        return "WITH (transactional = true) ";
    }
}
