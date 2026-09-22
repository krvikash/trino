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

import io.trino.Session;

import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;

final class TestDeltaLakeCompositeSplitsConnectorTest
        extends TestDeltaLakeConnectorTest
{
    @Override
    protected DeltaLakeQueryRunner.Builder createQueryRunnerBuilder()
    {
        return super.createQueryRunnerBuilder()
                .addDeltaProperty("delta.experimental.composite-splits.enabled", "true")
                // Write many small files, including the initial TPC-H tables, so that scans form composites
                .amendSession(session -> session.setCatalogSessionProperty(DELTA_CATALOG, "target_max_file_size", "1kB"));
    }

    @Override
    protected Session getFileCountSensitiveWriterSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(DELTA_CATALOG, "target_max_file_size", new DeltaLakeConfig().getTargetMaxFileSize().toString())
                .build();
    }
}
