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
package io.trino.plugin.jmx;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.jmx.JmxQueryRunner.createSession;
import static org.assertj.core.api.Assertions.assertThat;

final class TestJmxSingleNodeExecution
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(createSession())
                .setWorkerCount(1)
                .addExtraProperty("node-scheduler.include-coordinator", "false")
                .addExtraProperty("experimental.force-single-node-query", "true")
                .build();
        queryRunner.installPlugin(new JmxPlugin());
        queryRunner.createCatalog("jmx", "jmx");
        return queryRunner;
    }

    @Test
    void testJmxQueryWithSingleNodeExecutionMode()
    {
        // JMX creates one split per node, each pinned to that node (isRemotelyAccessible=false).
        // Verify SNEM is correctly disabled for JMX queries so all nodes report data.
        assertThat(query("SELECT count(DISTINCT node) FROM \"java.lang:type=Runtime\""))
                .matches("VALUES BIGINT '2'");
    }
}
