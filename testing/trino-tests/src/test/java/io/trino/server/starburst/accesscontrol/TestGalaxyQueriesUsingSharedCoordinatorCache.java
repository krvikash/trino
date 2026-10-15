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
package io.trino.server.starburst.accesscontrol;

import com.google.common.collect.ImmutableSet;
import io.starburst.stargate.accesscontrol.client.RevokeEntityPrivilege;
import io.starburst.stargate.accesscontrol.privilege.Privilege;
import io.starburst.stargate.id.CatalogId;
import io.starburst.stargate.id.ColumnId;
import io.starburst.stargate.id.RoleName;
import io.trino.server.starburst.accesscontrol.GalaxyAccountPermissionsCache.PublishedValidationStatistics;
import org.junit.jupiter.api.Test;

import static io.trino.server.starburst.security.GalaxyIdentity.toDispatchSession;
import static org.assertj.core.api.Assertions.assertThat;

public class TestGalaxyQueriesUsingSharedCoordinatorCache
        extends BaseTestGalaxyStaticCatalogQueries
{
    public TestGalaxyQueriesUsingSharedCoordinatorCache()
    {
        useSharedPermissionsCache = true;
    }

    @Test
    public void testSharedCacheErrorRecovery()
    {
        RoleName roleName = new RoleName("accountadmin");
        assertUpdate("CREATE SCHEMA memory.cache_test");
        try {
            CatalogId memoryCatalogId = testingAccountClient.getOrCreateCatalog("memory");
            assertUpdate("CREATE TABLE memory.cache_test.test_table(col1 int, col2 varchar)");
            assertUpdate("INSERT INTO memory.cache_test.test_table VALUES (1, 'one'), (2, 'two')", 2);

            // Capture the GalaxyAccountPermissionsCache counters
            PublishedValidationStatistics statisticsBefore = galaxyAccountPermissionsCache.getValidationStatistics();

            // Run a query that references a non-existent column
            assertQueryFails("SELECT col3 FROM memory.cache_test.test_table", "line 1:8: Column 'col3' cannot be resolved.*");

            PublishedValidationStatistics statisticsAfter = galaxyAccountPermissionsCache.getValidationStatistics();

            // Show that analysis got an exception, but the cache was up-to-date, and because of that, analysis was not re-run
            assertThat(statisticsAfter.getAnalysisExceptions() - statisticsBefore.getAnalysisExceptions()).isEqualTo(1);
            assertThat(statisticsAfter.getAnalysisExceptionsWithUpToDateCache() - statisticsBefore.getAnalysisExceptionsWithUpToDateCache()).isEqualTo(1);
            assertThat(statisticsAfter.getAnalysisReruns() - statisticsBefore.getAnalysisReruns()).isEqualTo(0);

            assertThat(query("SELECT col1 FROM memory.cache_test.test_table WHERE col2 = 'one'")).matches("SELECT 1");

            // Revoke SELECT on all columns of memory.cache_test.test_table to invalidate the cache
            getTrinoSecurityApi().revokeEntityPrivileges(
                    toDispatchSession(getSession()),
                    new ColumnId(memoryCatalogId, "cache_test", "test_table", "*"),
                    ImmutableSet.of(new RevokeEntityPrivilege(Privilege.SELECT, roleName, false)));

            // References to column col1 now fail.
            statisticsBefore = galaxyAccountPermissionsCache.getValidationStatistics();
            assertQueryFails("SELECT col1 FROM memory.cache_test.test_table WHERE col2 = 'one'", "Access Denied: Cannot select from columns \\[col2, col1] in table or view memory.cache_test.test_table.*");
            statisticsAfter = galaxyAccountPermissionsCache.getValidationStatistics();

            // Show that no exception occurred during the first analysis, because the cache was not up-to-date so based on
            // cached privileges, it seemed that the query succeeded.
            assertThat(statisticsAfter.getAnalysisExceptions() - statisticsBefore.getAnalysisExceptions()).isEqualTo(0);

            // However, analysis depended on a cache entry found to be invalid, so analysis was re-run.
            // The invalid entry was, as expected, GetEntityPrivileges
            assertThat(statisticsAfter.getAnalysisExceptionsWithUpToDateCache() - statisticsBefore.getAnalysisExceptionsWithUpToDateCache()).isEqualTo(0);
            assertThat(statisticsAfter.getSomeInvalidValidateCount() - statisticsBefore.getSomeInvalidValidateCount()).isEqualTo(1);
            assertThat(statisticsAfter.getInvalidResultsByType().get("GetEntityPrivileges")).isGreaterThan(0);
            assertThat(statisticsAfter.getAnalysisReruns() - statisticsBefore.getAnalysisReruns()).isEqualTo(1);
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS memory.cache_test CASCADE");
        }
    }
}
