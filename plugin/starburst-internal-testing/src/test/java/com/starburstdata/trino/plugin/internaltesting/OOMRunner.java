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
package com.starburstdata.trino.plugin.internaltesting;

import io.airlift.log.Logger;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class OOMRunner
{
    static final String OOM_SCHEMA_NAME = "oom";
    private static final Logger log = Logger.get(OOMRunner.class);

    private OOMRunner()
    {
    }

    @SuppressWarnings("CallToSystemExit")
    static void main(String[] args)
    {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: RunOOM <coordinator|worker>");
        }
        String target = args[0];
        try {
            log.info("Creating a query runner with a single worker");
            QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().setCatalog("testing").build())
                    .setWorkerCount(1)
                    .build();
            log.info("Triggering OOM for a %s ...".formatted(target));
            queryRunner.installPlugin(new InternalTestingPlugin());
            queryRunner.createCatalog("testing", "starburst_internal_testing");
            MaterializedResult result = queryRunner.execute("SELECT * FROM %s.%s".formatted(OOM_SCHEMA_NAME, target));
            System.out.println(result);
        }
        catch (Exception e) {
            log.error(e, "Error, shutting down OOM memory allocation");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
