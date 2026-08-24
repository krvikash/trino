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
package io.trino.plugin.sas.substitution;

import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.sas.SasQueryRunner;
import io.trino.plugin.sas.SasTpchLoader;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.ResultWithQueryId;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD)
public class TestIcebergMvOnSasSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    private Path dataDirectory;

    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        dataDirectory = Files.createTempDirectory("sas-mv-substitution-");
        closeAfterClass(() -> deleteRecursively(dataDirectory, ALLOW_INSECURE));

        return SasQueryRunner.builder(dataDirectory)
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        return new CatalogSchemaName(SasQueryRunner.CATALOG, SasQueryRunner.TPCH_SCHEMA);
    }

    // The SAS schema is a directory discovered dynamically as files are written, so there is no
    // Trino-side source schema to create.
    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }

    @Override
    protected void createOrdersTable()
    {
        // Bounded varchar casts keep the CHARACTER columns at their exact width, so no trailing-space
        // padding leaks into equality/range predicates on orderdate and orderstatus.
        loadTable(
                ordersTable,
                """
                SELECT
                  orderkey,
                  custkey,
                  CAST(orderdate AS varchar(10)) AS orderdate,
                  totalprice,
                  CAST(orderstatus AS varchar(1)) AS orderstatus
                FROM tpch.tiny.orders
                WHERE orderkey BETWEEN 20000 AND 20010
                """,
                OptionalInt.of(8));
    }

    @Override
    protected void createLineItemTable()
    {
        loadTable(
                lineitemTable,
                """
                SELECT
                  orderkey,
                  linenumber,
                  quantity,
                  extendedprice,
                  CAST(shipdate AS varchar(10)) AS shipdate
                FROM tpch.tiny.lineitem
                WHERE orderkey BETWEEN 20000 AND 20010
                """,
                OptionalInt.of(32));
    }

    @Override
    protected void insertIntoOrders(long id)
    {
        // The read-only SAS source cannot INSERT: rewrite the whole file as its current contents plus
        // the new row. The literal column types match what the existing file reports on read.
        loadTable(
                ordersTable,
                """
                SELECT * FROM %s
                UNION ALL
                SELECT CAST(%s AS double), CAST(400 AS double), CAST('1995-02-01' AS varchar(10)), CAST(99.99 AS double), CAST('N' AS varchar(1))
                """.formatted(ordersTable, id),
                OptionalInt.empty());
    }

    @Override
    protected void createNestedTypeTable(CatalogSchemaTableName table)
    {
        // No native nested type: store the sub-fields as JSON text in a CHARACTER column. The sub-field
        // expressions are json_extract_scalar projections above the scan (VARCHAR_JSON).
        loadTable(
                table,
                """
                SELECT * FROM (VALUES
                  (1, CAST('{"name":"Alice","age":30, "gender":"W"}' AS varchar(64))),
                  (2, CAST('{"name":"Bob","age":25, "gender":"M"}' AS varchar(64))),
                  (3, CAST('{"name":"Carol","age":40, "gender":"W"}' AS varchar(64)))
                ) AS t(id, info)
                """,
                OptionalInt.of(3));
    }

    @Override
    protected void createCoercionTable()
    {
        String values = coercionColumns().stream()
                .map(column -> "CAST(%s AS %s) as %s".formatted(column.insertValue(), column.columnType(), column.name()))
                .collect(Collectors.joining(", "));
        loadTable(coercionTable, "SELECT 1 as id_col, " + values, OptionalInt.of(1));
    }

    // The read-only SAS source cannot execute SQL, so a base table is dropped by deleting its file.
    @Override
    protected void dropSourceTable(CatalogSchemaTableName table)
    {
        Path file = dataDirectory.resolve(sourceSchemaName()).resolve(table.getSchemaTableName().getTableName() + ".sas7bdat");
        try {
            Files.deleteIfExists(file);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Test
    @Disabled("SAS is read-only: CREATE TABLE AS SELECT and ALTER TABLE ADD COLUMN are not supported on the source")
    @Override
    public void testSubstitutionAfterAddColumnToBaseTable() {}

    private void loadTable(CatalogSchemaTableName table, String sql, OptionalInt expectedUpdateCount)
    {
        Path schemaDir = dataDirectory.resolve(sourceSchemaName());
        try {
            Files.createDirectories(schemaDir);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (SasTpchLoader loader = new SasTpchLoader(getDistributedQueryRunner().getCoordinator(), schemaDir, table.getSchemaTableName().getTableName())) {
            ResultWithQueryId<Integer> result = loader.execute(sql);
            if (expectedUpdateCount.isPresent()) {
                assertThat(result.getResult()).isEqualTo(expectedUpdateCount.getAsInt());
            }
        }
    }
}
