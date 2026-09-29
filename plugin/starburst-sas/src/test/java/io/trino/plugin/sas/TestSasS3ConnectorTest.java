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
import io.trino.testing.QueryRunner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static io.trino.plugin.sas.SasFlociStorage.location;

/**
 * {@link BaseSasConnectorTest} with the TPCH files served from S3 (Floci) instead of the local
 * filesystem: the files are generated locally and uploaded before the query runner is handed out.
 */
public class TestSasS3ConnectorTest
        extends BaseSasConnectorTest
{
    private static final String BUCKET = "sas-connector-test";

    private SasFlociStorage storage;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        storage = closeAfterClass(SasFlociStorage.start());
        storage.createBucket(BUCKET);
        return SasQueryRunner.builder(Files.createTempDirectory("sas-s3-connector-test-"))
                .setCatalogProperties(storage.catalogProperties(location(BUCKET, "data")))
                .setInitialTablesUploader(this::uploadDataDirectory)
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    private void uploadDataDirectory(Path dataDirectory)
    {
        try {
            storage.uploadDirectory(dataDirectory, BUCKET, "data");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected Map<String, String> getBehaviorAlteringCatalogProperties()
    {
        // A prefix without objects lists no schemas, so the connector discovers no tables;
        // the s3.* properties of the catalog under test are kept as they are
        return ImmutableMap.of("sas.data-directory", location(BUCKET, "empty"));
    }
}
