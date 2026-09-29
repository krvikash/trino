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
package io.trino.tests.product.sas;

import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.testing.minio.MinioClient;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.SAS_S3;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * SAS connector reading from S3 (MinIO). The environment copies the {@code .sas7bdat} files into
 * the tests container; this class uploads them to the bucket the catalog points at.
 */
public class TestSasS3
        extends ProductTest
{
    private static final String BUCKET = "sas-bucket";
    private static final String BUCKET_PREFIX = "data";
    private static final Path DATA_DIRECTORY = Path.of("/docker/sas-data");

    @BeforeMethodWithContext
    public void setUp()
    {
        List<Path> files;
        try (Stream<Path> walk = Files.walk(DATA_DIRECTORY)) {
            files = walk.filter(Files::isRegularFile).collect(toImmutableList());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (MinioClient client = new MinioClient()) {
            client.ensureBucketExists(BUCKET);
            for (Path file : files) {
                try {
                    client.putObject(BUCKET, Files.readAllBytes(file), BUCKET_PREFIX + "/" + DATA_DIRECTORY.relativize(file));
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Failed to upload " + file, e);
                }
            }
        }
    }

    @Test(groups = {SAS_S3, PROFILE_SPECIFIC_TESTS})
    public void testShowSchemasAndTables()
    {
        assertThat(onTrino().executeQuery("SHOW SCHEMAS FROM sas"))
                .contains(row("schema1"));
        assertThat(onTrino().executeQuery("SHOW TABLES FROM sas.schema1"))
                .containsOnly(row("colon"));
    }

    @Test(groups = {SAS_S3, PROFILE_SPECIFIC_TESTS})
    public void testSelectCount()
    {
        assertThat(onTrino().executeQuery("SELECT count(*) FROM sas.schema1.colon"))
                .containsOnly(row(15564L));
    }

    @Test(groups = {SAS_S3, PROFILE_SPECIFIC_TESTS})
    public void testShowColumns()
    {
        assertThat(onTrino().executeQuery("SHOW COLUMNS FROM sas.schema1.colon"))
                .hasRowsCount(13);
    }
}
