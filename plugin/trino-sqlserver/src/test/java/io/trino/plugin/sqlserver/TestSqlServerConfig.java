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
package io.trino.plugin.sqlserver;

import com.google.common.collect.ImmutableMap;
import jakarta.validation.constraints.AssertTrue;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static io.trino.plugin.sqlserver.SqlServerConfig.TransactionIsolationLevel.READ_UNCOMMITTED;

public class TestSqlServerConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(SqlServerConfig.class)
                .setBulkCopyForWrite(false)
                .setBulkCopyForWriteLockDestinationTable(false)
                .setSnapshotIsolationDisabled(false)
                .setTransactionIsolationLevel(null)
                .setStoredProcedureTableFunctionEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("sqlserver.bulk-copy-for-write.enabled", "true")
                .put("sqlserver.bulk-copy-for-write.lock-destination-table", "true")
                .put("sqlserver.transaction-isolation-level", "READ_UNCOMMITTED")
                .put("sqlserver.stored-procedure-table-function-enabled", "true")
                .buildOrThrow();

        SqlServerConfig expected = new SqlServerConfig()
                .setBulkCopyForWrite(true)
                .setBulkCopyForWriteLockDestinationTable(true)
                .setStoredProcedureTableFunctionEnabled(true)
                .setTransactionIsolationLevel(READ_UNCOMMITTED);

        assertFullMapping(properties, expected, Set.of("sqlserver.snapshot-isolation.disabled"));
    }

    @Test
    public void testTransactionIsolationConfigConsistentValidation()
    {
        assertValidates(new SqlServerConfig()
                .setSnapshotIsolationDisabled(true));

        assertValidates(new SqlServerConfig()
                .setTransactionIsolationLevel(READ_UNCOMMITTED));

        assertFailsValidation(
                new SqlServerConfig()
                        .setSnapshotIsolationDisabled(true)
                        .setTransactionIsolationLevel(READ_UNCOMMITTED),
                "transactionIsolationConfigConsistent",
                "Set only one of sqlserver.transaction-isolation-level or the legacy sqlserver.snapshot-isolation.disabled",
                AssertTrue.class);
    }
}
