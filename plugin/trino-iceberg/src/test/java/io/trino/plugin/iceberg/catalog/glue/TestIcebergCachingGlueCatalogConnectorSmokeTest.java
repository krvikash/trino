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
package io.trino.plugin.iceberg.catalog.glue;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.plugin.iceberg.BaseIcebergConnectorSmokeTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import org.apache.iceberg.FileFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergTestUtils.checkParquetFileSorting;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/*
 * Tests BaseIcebergConnectorSmokeTest with Glue metastore caching enabled.
 *
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
final class TestIcebergCachingGlueCatalogConnectorSmokeTest
        extends BaseIcebergConnectorSmokeTest
{
    private final String bucketName;
    private final String schemaName;
    private final GlueClient glueClient;

    public TestIcebergCachingGlueCatalogConnectorSmokeTest()
    {
        super(FileFormat.PARQUET);
        this.bucketName = requireEnv("S3_BUCKET");
        this.schemaName = "test_iceberg_caching_smoke_" + randomNameSuffix();
        this.glueClient = GlueClient.create();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.file-format", format.name(),
                                "iceberg.catalog.type", "glue",
                                "fs.s3.enabled", "true",
                                "hive.metastore.glue.default-warehouse-dir", schemaPath(),
                                "iceberg.register-table-procedure.enabled", "true",
                                "iceberg.writer-sort-buffer-size", "1MB",
                                "iceberg.allowed-extra-properties", "write.metadata.delete-after-commit.enabled,write.metadata.previous-versions-max",
                                "iceberg.glue.metastore-cache.ttl", "10m"))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(REQUIRED_TPCH_TABLES)
                                .withSchemaName(schemaName)
                                .build())
                .build();
    }

    @AfterAll
    public void cleanup()
    {
        computeActual("SHOW TABLES").getMaterializedRows()
                .forEach(table -> getQueryRunner().execute("DROP TABLE " + table.getField(0)));
        getQueryRunner().execute("DROP SCHEMA IF EXISTS " + schemaName);

        glueClient.close();
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("""
                        CREATE TABLE iceberg\\.%s\\.region \\(
                           regionkey bigint,
                           name varchar,
                           comment varchar
                        \\)
                        WITH \\(
                           format = 'PARQUET',
                           format_version = 2,
                           location = '%s/%s\\.db/region-.*'
                        \\)""".formatted(schemaName, schemaPath(), schemaName));
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Glue catalogs");
    }

    @Override
    protected void dropTableFromCatalog(String tableName)
    {
        glueClient.deleteTable(x -> x.databaseName(schemaName).name(tableName));
        assertThatThrownBy(() -> glueClient.getTable(x -> x.databaseName(schemaName).name(tableName)).table())
                .isInstanceOf(EntityNotFoundException.class);
    }

    @Override
    protected String getMetadataLocation(String tableName)
    {
        return glueClient.getTable(x -> x.databaseName(schemaName).name(tableName)).table().parameters().get("metadata_location");
    }

    @Override
    protected void deleteDirectory(String location)
    {
        try (S3Client s3 = S3Client.create()) {
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(location)
                    .build();
            s3.listObjectsV2Paginator(listObjectsRequest).stream()
                    .forEach(listObjectsResponse -> {
                        List<String> keys = listObjectsResponse.contents().stream().map(S3Object::key).collect(toImmutableList());
                        if (!keys.isEmpty()) {
                            DeleteObjectsRequest deleteObjectsRequest = DeleteObjectsRequest.builder()
                                    .bucket(bucketName)
                                    .delete(builder -> builder.objects(keys.stream()
                                            .map(key -> ObjectIdentifier.builder().key(key).build())
                                            .toList()).quiet(true))
                                    .build();
                            s3.deleteObjects(deleteObjectsRequest);
                        }
                    });

            assertThat(s3.listObjects(ListObjectsRequest.builder().bucket(bucketName).prefix(location).build()).contents()).isEmpty();
        }
    }

    @Override
    protected boolean isFileSorted(Location path, String sortColumnName)
    {
        return checkParquetFileSorting(fileSystem.newInputFile(path), sortColumnName);
    }

    @Override
    protected String schemaPath()
    {
        return format("s3://%s/%s", bucketName, schemaName);
    }

    @Override
    protected boolean locationExists(String location)
    {
        try (S3Client s3 = S3Client.create()) {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(location)
                    .maxKeys(1)
                    .build();
            return !s3.listObjectsV2(request).contents().isEmpty();
        }
    }
}
