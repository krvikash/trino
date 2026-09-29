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
import java.util.Map;

public class TestSasConnectorTest
        extends BaseSasConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return SasQueryRunner.builder(Files.createTempDirectory("sas-connector-test-"))
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected Map<String, String> getBehaviorAlteringCatalogProperties()
    {
        try {
            // An empty directory passes LocalFileSystem's existence check but has no schema subdirectories,
            // so the connector discovers no tables
            return ImmutableMap.of("sas.data-directory", Files.createTempDirectory("sas-empty-data-directory-").toUri().toString());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
