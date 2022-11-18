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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.metastore.HiveMetastore;

import java.io.File;
import java.util.Map;

import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;

public class TestDeltaLakeRegisterTableProcedureWithFile
        extends BaseDeltaLakeRegisterTableProcedureTest
{
    @Override
    protected Map<String, String> getConnectorProperties(String dataDirectory)
    {
        return ImmutableMap.of(
                "hive.metastore", "file",
                "hive.metastore.catalog.dir", dataDirectory);
    }

    @Override
    protected HiveMetastore createTestMetastore(String dataDirectory)
    {
        return createTestingFileHiveMetastore(new File(dataDirectory));
    }
}
