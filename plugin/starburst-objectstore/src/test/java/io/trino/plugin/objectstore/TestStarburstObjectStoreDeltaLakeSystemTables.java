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
package io.trino.plugin.objectstore;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.ClassPath;
import io.trino.plugin.deltalake.TestDeltaLakeSystemTables;
import io.trino.testing.DistributedQueryRunner;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.trino.plugin.base.util.Closables.closeAllSuppress;
import static io.trino.plugin.objectstore.StarburstObjectStoreConnectorFactory.STARBURST_OBJECTSTORE;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

public class TestStarburstObjectStoreDeltaLakeSystemTables
        extends TestDeltaLakeSystemTables
{
    @Override
    protected DistributedQueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(
                        testSessionBuilder()
                                .setCatalog("objectstore")
                                .setSchema("tpch")
                                .build())
                .build();

        Path dataDirectory = queryRunner.getCoordinator().getBaseDataDir().resolve("delta_lake_data");
        verify(dataDirectory.toFile().mkdirs());

        try {
            queryRunner.installPlugin(new TestingObjectStorePlugin(STARBURST_OBJECTSTORE, Optional.empty(), Optional.empty(), Optional.of(dataDirectory)));
            queryRunner.createCatalog("objectstore", STARBURST_OBJECTSTORE, ImmutableMap.<String, String>builder()
                    .put("great-lakes.table-type", TableType.DELTA.name())
                    .put("hive.metastore", "file")
                    .put("hive.metastore.catalog.dir", "file://" + dataDirectory)
                    .put("delta.register-table-procedure.enabled", "true")
                    .put("delta.enable-non-concurrent-writes", "true")
                    .put("fs.hadoop.enabled", "true")
                    .buildOrThrow());
            queryRunner.execute("CREATE SCHEMA objectstore.tpch");
        }
        catch (Throwable e) {
            closeAllSuppress(e, queryRunner);
            throw e;
        }

        return queryRunner;
    }

    @Override
    protected String copyTableAndGetLocation(String tableName)
            throws IOException
    {
        String resourcePath = "databricks133/partition_values_parsed_case_sensitive";
        Path tableLocation = Files.createTempDirectory(tableName);

        ClassLoader classLoader = getClass().getClassLoader();
        ClassPath classPath = ClassPath.from(classLoader);

        for (ClassPath.ResourceInfo resourceInfo : classPath.getResources()) {
            if (resourceInfo.getResourceName().startsWith(resourcePath)) {
                String relativePath = resourceInfo.getResourceName().substring(resourcePath.length() + 1);
                Path target = tableLocation.resolve(relativePath);
                Files.createDirectories(target.getParent());
                try (InputStream in = classLoader.getResourceAsStream(resourceInfo.getResourceName())) {
                    Files.copy(in, target, REPLACE_EXISTING);
                }
            }
        }
        return tableLocation.toUri().toString();
    }
}
