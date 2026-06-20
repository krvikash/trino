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
package io.trino.plugin.iceberg.procedure;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

import java.io.File;
import java.nio.file.Files;

import static io.trino.plugin.hive.metastore.glue.GlueConverter.getTableType;

/*
 * The test currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
final class TestIcebergGlueFlushMetadataCacheProcedure
        extends BaseTestIcebergFlushMetadataCacheProcedure
{
    private GlueClient glueClient;
    private File schemaDirectory;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        glueClient = closeAfterClass(GlueClient.create());
        schemaDirectory = Files.createTempDirectory("test_iceberg").toFile();
        schemaDirectory.deleteOnExit();

        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.<String, String>builder()
                                .put("iceberg.catalog.type", "glue")
                                .put("fs.hadoop.enabled", "true")
                                .put("hive.metastore.glue.default-warehouse-dir", schemaDirectory.getAbsolutePath())
                                .put("iceberg.glue.metastore-cache.ttl", "10m")
                                .buildOrThrow())
                .disableSchemaInitializer()
                .build();
    }

    @AfterAll
    public void tearDown()
    {
        glueClient.close();
    }

    @Override
    protected String getSchemaLocation()
    {
        return schemaDirectory.getAbsolutePath();
    }

    @Override
    protected void renameTableOutsideTrino(String schemaName, String sourceTableName, String targetTableName)
    {
        Table table = glueClient.getTable(builder -> builder.databaseName(schemaName).name(sourceTableName)).table();
        TableInput tableInput = TableInput.builder()
                .name(targetTableName)
                .parameters(table.parameters())
                .storageDescriptor(table.storageDescriptor())
                .tableType(getTableType(table))
                .build();
        glueClient.createTable(builder -> builder.databaseName(schemaName).tableInput(tableInput));
        glueClient.deleteTable(builder -> builder.databaseName(schemaName).name(sourceTableName));
    }
}
