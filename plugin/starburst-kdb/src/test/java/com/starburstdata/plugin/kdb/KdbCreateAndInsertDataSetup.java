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
package com.starburstdata.plugin.kdb;

import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.TemporaryRelation;

import java.util.List;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.util.Objects.requireNonNull;

/**
 * Each {@link ColumnSetup#getInputLiteral()} is treated as a q expression for a single
 * scalar value; the DataSetup wraps it with {@code enlist} to produce a 1-row table column.
 */
final class KdbCreateAndInsertDataSetup
        implements DataSetup
{
    private final KdbClient kdbClient;
    private final String tableNamePrefix;

    KdbCreateAndInsertDataSetup(KdbClient kdbClient, String tableNamePrefix)
    {
        this.kdbClient = requireNonNull(kdbClient, "kdbClient is null");
        this.tableNamePrefix = requireNonNull(tableNamePrefix, "tableNamePrefix is null");
    }

    @Override
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        String tableName = tableNamePrefix + randomNameSuffix();

        // Build: tableName:([]col_0:enlist <literal0>; col_1:enlist <literal1>; ...)
        var q = new StringBuilder(tableName).append(":([]");
        for (int i = 0; i < inputs.size(); i++) {
            if (i > 0) {
                q.append("; ");
            }
            q.append("col_").append(i).append(":enlist ").append(inputs.get(i).getInputLiteral());
        }
        q.append(")");

        kdbClient.execute(q.toString());

        return new KdbTemporaryTable(kdbClient, tableName);
    }

    private static final class KdbTemporaryTable
            implements TemporaryRelation
    {
        private final KdbClient kdbClient;
        private final String name;

        KdbTemporaryTable(KdbClient kdbClient, String name)
        {
            this.kdbClient = kdbClient;
            this.name = name;
        }

        @Override
        public String getName()
        {
            return name;
        }

        @Override
        public void close()
        {
            try {
                kdbClient.execute("delete " + name + " from `.");
            }
            catch (Exception ignored) {
                // cleanup failed
            }
        }
    }
}
