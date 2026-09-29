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
package io.trino.plugin.sas;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.sas.SasFlociStorage.location;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage of a {@code sas.data-directory=s3://...} catalog against Floci. The
 * {@code sas_local} catalog serves the same files from the local filesystem and is the baseline
 * every S3 result is compared against.
 */
final class TestSasS3Queries
        extends AbstractTestQueryFramework
{
    private static final String BUCKET = "sas-query-test";
    private static final String ROOT_BUCKET = "sas-root-bucket";
    private static final String PREFIX = "sas/root";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Path resources = Path.of("src/test/resources").toAbsolutePath();

        SasFlociStorage storage = closeAfterClass(SasFlociStorage.start());
        storage.createBucket(BUCKET);
        storage.uploadDirectory(resources, BUCKET, PREFIX);
        // the AWS console "Create folder" action leaves zero-byte marker objects behind
        storage.putObject(BUCKET, PREFIX + "/schema1/", new byte[0]);
        storage.createBucket(ROOT_BUCKET);
        storage.uploadDirectory(resources, ROOT_BUCKET, "");

        DistributedQueryRunner queryRunner = SasQueryRunner.builder(resources)
                .setCatalogProperties(storage.catalogProperties(location(BUCKET, PREFIX)))
                .build();
        try {
            queryRunner.createCatalog("sas_local", "sas", ImmutableMap.of(
                    "sas.data-directory", resources.toUri().toString()));
            queryRunner.createCatalog("sas_fine_split", "sas", ImmutableMap.<String, String>builder()
                    .putAll(storage.catalogProperties(location(BUCKET, PREFIX)))
                    .put("sas.min-page-per-split", "1")
                    .buildOrThrow());
            queryRunner.createCatalog("sas_json", "sas", ImmutableMap.<String, String>builder()
                    .putAll(storage.catalogProperties(location(BUCKET, PREFIX)))
                    .put("sas.mapping-type", "JSON")
                    .put("sas.json-file", resources.resolve("mapping_sas.json").toString())
                    .buildOrThrow());
            queryRunner.createCatalog("sas_trailing_slash", "sas", storage.catalogProperties(location(BUCKET, PREFIX) + "/"));
            queryRunner.createCatalog("sas_bucket_root", "sas", storage.catalogProperties(location(ROOT_BUCKET, "")));
            queryRunner.createCatalog("sas_uppercase_scheme", "sas", storage.catalogProperties(location(BUCKET, PREFIX).replace("s3://", "S3://")));
            return queryRunner;
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }
    }

    @Test
    void testShowSchemas()
    {
        assertThat(query("SHOW SCHEMAS FROM sas"))
                .matches("VALUES VARCHAR 'data', 'information_schema', 'prod', 'schema1', 'schema2', 'schema3'");
    }

    @Test
    void testShowTables()
    {
        assertThat(query("SHOW TABLES FROM sas.schema1"))
                .matches("VALUES VARCHAR 'colon'");
        assertThat(query("SHOW TABLES FROM sas.data"))
                .matches("VALUES VARCHAR 'year2024/reports/sales_report'");
    }

    @Test
    void testSelectMatchesLocalFilesystem()
    {
        assertThat(query("SELECT * FROM sas.schema1.colon"))
                .matches("SELECT * FROM sas_local.schema1.colon");
        assertThat(query("SELECT * FROM sas.prod.param_gen_nn1"))
                .matches("SELECT * FROM sas_local.prod.param_gen_nn1");
        assertThat(query("SELECT * FROM sas.data.\"year2024/reports/sales_report\""))
                .matches("SELECT * FROM sas_local.data.\"year2024/reports/sales_report\"");
    }

    @Test
    void testCaseInsensitiveLookup()
    {
        // stored as SCHEMA3/UPPER.sas7bdat
        assertThat(query("SELECT count(*) FROM sas.schema3.upper"))
                .matches("VALUES BIGINT '15564'");
        assertThat(query("SELECT count(*) FROM sas.SCHEMA1.COLON"))
                .matches("VALUES BIGINT '15564'");
    }

    @Test
    void testDescribe()
    {
        assertThat(query("DESCRIBE sas.schema1.colon"))
                .matches("DESCRIBE sas_local.schema1.colon");
    }

    @Test
    void testMissingSchemaAndTable()
    {
        assertQueryFails("SELECT * FROM sas.missing.colon", ".*Schema 'missing' does not exist.*");
        assertQueryFails("SELECT * FROM sas.schema1.missing", ".*Table 'sas.schema1.missing' does not exist.*");
    }

    @Test
    void testMultiSplitReadsMatchSingleSplit()
    {
        // colon is uncompressed with 7 pages: every split past the first has to seek into the object
        assertThat(query("SELECT * FROM sas_fine_split.schema1.\"colon$7\""))
                .matches("SELECT * FROM sas_local.schema1.colon");
        assertThat(query("SELECT sum(age), sum(surv_mm) FROM sas_fine_split.schema1.\"colon$3\""))
                .matches("SELECT sum(age), sum(surv_mm) FROM sas_local.schema1.colon");
        // param_gen_nn1 is compressed with 3 pages
        assertThat(query("SELECT * FROM sas_fine_split.prod.\"param_gen_nn1$3\""))
                .matches("SELECT * FROM sas_local.prod.param_gen_nn1");
        for (int splits : new int[] {2, 5, 100}) {
            assertThat(query("SELECT count(*) FROM sas_fine_split.schema1.\"colon$%s\"".formatted(splits)))
                    .matches("VALUES BIGINT '15564'");
        }
    }

    @Test
    void testJsonMapping()
    {
        assertThat(query("SHOW SCHEMAS FROM sas_json"))
                .matches("VALUES VARCHAR 'information_schema', 'mapping_schema', 'mapping_table'");
        assertThat(query("SELECT count(*) FROM sas_json.mapping_table.colon"))
                .matches("VALUES BIGINT '15564'");
        assertThat(query("SHOW TABLES FROM sas_json.mapping_schema"))
                .matches("VALUES VARCHAR 'colon'");
        assertThat(query("SELECT * FROM sas_json.mapping_schema.colon"))
                .matches("SELECT * FROM sas_local.schema1.colon");
    }

    @Test
    void testTrailingSlashInDataDirectory()
    {
        assertThat(query("SHOW SCHEMAS FROM sas_trailing_slash"))
                .matches("SHOW SCHEMAS FROM sas");
        assertThat(query("SELECT count(*) FROM sas_trailing_slash.schema1.colon"))
                .matches("VALUES BIGINT '15564'");
    }

    @Test
    void testBucketRootDataDirectory()
    {
        assertThat(query("SHOW SCHEMAS FROM sas_bucket_root"))
                .matches("SHOW SCHEMAS FROM sas");
        assertThat(query("SELECT * FROM sas_bucket_root.schema2.colon"))
                .matches("SELECT * FROM sas_local.schema2.colon");
    }

    @Test
    void testUppercaseSchemeInDataDirectory()
    {
        // URI preserves the scheme's case verbatim; S3Location only accepts lowercase s3/s3a/s3n,
        // so the root location must be normalized rather than built from the raw URI
        assertThat(query("SHOW SCHEMAS FROM sas_uppercase_scheme"))
                .matches("SHOW SCHEMAS FROM sas");
        assertThat(query("SELECT * FROM sas_uppercase_scheme.schema1.colon"))
                .matches("SELECT * FROM sas_local.schema1.colon");
    }
}
