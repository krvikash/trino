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
package io.trino.plugin.opensearch.substitution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.substitution.AbstractIcebergMvSubstitutionTest;
import io.trino.plugin.opensearch.OpenSearchQueryRunner;
import io.trino.plugin.opensearch.OpenSearchServer;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.SqlExecutor;
import org.apache.http.HttpHost;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static io.trino.plugin.opensearch.OpenSearchServer.OPENSEARCH_IMAGE;
import static java.lang.String.format;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * End-to-end MV substitution test with OpenSearch as the source. The OpenSearch connector is
 * read-only, so unlike the JDBC-backed sources the base tables cannot be created, populated or
 * dropped through Trino (or a backend SQL executor); they are seeded directly as OpenSearch
 * indices through the REST client. The test then builds an Iceberg-backed materialized view over
 * such an index and asserts that base-table queries are redirected to the MV storage table.
 */
@Execution(SAME_THREAD)
public class TestIcebergMvOnOpenSearchSourceSubstitution
        extends AbstractIcebergMvSubstitutionTest
{
    private RestClient client;

    @Override
    protected QueryRunner createSourceQueryRunner(Session defaultSession, CatalogSchemaName sourceSchema)
            throws Exception
    {
        OpenSearchServer server = closeAfterClass(new OpenSearchServer(OPENSEARCH_IMAGE, false, ImmutableMap.of()));
        client = closeAfterClass(RestClient.builder(HttpHost.create(server.getAddress().toString())).build());
        return OpenSearchQueryRunner.builder(server.getAddress())
                .addExtraProperty("materialized-view-substitution.support.enabled", "true")
                .build();
    }

    @Override
    protected CatalogSchemaName sourceSchema()
    {
        // "tpch" is the OpenSearch connector's default schema name (see OpenSearchQueryRunner).
        return new CatalogSchemaName("opensearch", "tpch");
    }

    // OpenSearchQueryRunner already provisions the tpch catalog, and the OpenSearch schema is
    // virtual (indices are discovered dynamically), so no Trino-side source schema is created.
    @Override
    protected boolean addTpchConnector()
    {
        return false;
    }

    @Override
    protected boolean createSourceSchema()
    {
        return false;
    }

    @Override
    protected void createOrdersTable()
    {
        String index = indexName(ordersTable);
        createIndex(index,
                """
                {"orderkey":{"type":"long"},"custkey":{"type":"long"},"orderdate":{"type":"keyword"},\
                "totalprice":{"type":"double"},"orderstatus":{"type":"keyword"}}""");
        indexDocument(index, 20000, orderDoc(20000, 100, "1995-01-05", 100.0, "O"));
        indexDocument(index, 20001, orderDoc(20001, 101, "1995-01-10", 200.0, "F"));
        indexDocument(index, 20002, orderDoc(20002, 102, "1995-01-15", 300.0, "P"));
        indexDocument(index, 20003, orderDoc(20003, 103, "1995-01-15", 400.0, "F"));
        indexDocument(index, 20004, orderDoc(20004, 104, "1995-01-20", 500.0, "O"));
        indexDocument(index, 20005, orderDoc(20005, 105, "1995-01-25", 600.0, "F"));
        indexDocument(index, 20006, orderDoc(20006, 106, "1995-01-30", 700.0, "P"));
        indexDocument(index, 20007, orderDoc(20007, 107, "1995-02-01", 800.0, "O"));
    }

    @Override
    protected void createLineItemTable()
    {
        String index = indexName(lineitemTable);
        createIndex(index,
                """
                {"orderkey":{"type":"long"},"linenumber":{"type":"long"},"quantity":{"type":"double"},\
                "extendedprice":{"type":"double"},"shipdate":{"type":"keyword"}}""");
        indexDocument(index, "20000-1", lineDoc(20000, 1, 10.0, 1000.0, "1995-02-01"));
        indexDocument(index, "20000-2", lineDoc(20000, 2, 5.0, 500.0, "1995-02-05"));
        indexDocument(index, "20001-1", lineDoc(20001, 1, 7.0, 700.0, "1995-02-10"));
        indexDocument(index, "20002-1", lineDoc(20002, 1, 3.0, 300.0, "1995-02-15"));
    }

    @Override
    protected void insertIntoOrders(long id)
    {
        indexDocument(indexName(ordersTable), id, orderDoc(id, 400, "1995-02-01", 99.99, "N"));
    }

    @Override
    protected void createNestedTypeTable(CatalogSchemaTableName table)
    {
        // OpenSearch exposes a nested object as a Trino ROW, so the sub-field is a struct
        // dereference (see subFieldTestContext()).
        String index = indexName(table);
        createIndex(index,
                """
                {"id":{"type":"long"},"info":{"properties":\
                {"name":{"type":"keyword"},"age":{"type":"integer"},"gender":{"type":"keyword"}}}}""");
        indexDocument(index, 1, "{\"id\":1,\"info\":{\"name\":\"Alice\",\"age\":30,\"gender\":\"W\"}}");
        indexDocument(index, 2, "{\"id\":2,\"info\":{\"name\":\"Bob\",\"age\":25,\"gender\":\"M\"}}");
        indexDocument(index, 3, "{\"id\":3,\"info\":{\"name\":\"Carol\",\"age\":40,\"gender\":\"W\"}}");
    }

    @Override
    protected List<CoercionColumn> coercionColumns()
    {
        return ImmutableList.of(new CoercionColumn("c_smallint", "smallint", "42", "abs(%s)"));
    }

    @Override
    protected void createCoercionTable()
    {
        String index = indexName(coercionTable);
        createIndex(index,
                """
                {"id_col":{"type":"long"}, "c_smallint":{"type":"short"}}""");
        indexDocument(index, 20007,
                """
                {"id_col":%d,"c_smallint":"%d"}""".formatted(1, 42));
    }

    @Override
    protected SubFieldTestContext subFieldTestContext()
    {
        return SubFieldTestContext.ROW;
    }

    @Override
    protected String sourceTableReference(CatalogSchemaTableName sourceTable)
    {
        return indexName(sourceTable);
    }

    @Override
    protected SqlExecutor sourceSqlExecutor()
    {
        throw new UnsupportedOperationException("Opensearch connector is read-only");
    }

    @Override
    protected void dropSourceTable(CatalogSchemaTableName sourceTable)
    {
        deleteIndex(sourceTable.getSchemaTableName().getTableName());
    }

    @Test
    @Disabled("OpenSearch is read-only: CREATE TABLE AS SELECT and ALTER TABLE ADD COLUMN are not supported on the source")
    @Override
    public void testSubstitutionAfterAddColumnToBaseTable() {}

    private static String indexName(CatalogSchemaTableName table)
    {
        return table.getSchemaTableName().getTableName();
    }

    private static String orderDoc(long orderkey, long custkey, String orderdate, double totalprice, String orderstatus)
    {
        return "{\"orderkey\":%d,\"custkey\":%d,\"orderdate\":\"%s\",\"totalprice\":%s,\"orderstatus\":\"%s\"}"
                .formatted(orderkey, custkey, orderdate, totalprice, orderstatus);
    }

    private static String lineDoc(long orderkey, long linenumber, double quantity, double extendedprice, String shipdate)
    {
        return "{\"orderkey\":%d,\"linenumber\":%d,\"quantity\":%s,\"extendedprice\":%s,\"shipdate\":\"%s\"}"
                .formatted(orderkey, linenumber, quantity, extendedprice, shipdate);
    }

    private void createIndex(String index, String propertiesJson)
    {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity("{\"mappings\":{\"properties\":%s}}".formatted(propertiesJson));
        performRequest(request);
    }

    private void indexDocument(String index, Object id, String docJson)
    {
        Request request = new Request("PUT", format("/%s/_doc/%s?refresh=true", index, id));
        request.setJsonEntity(docJson);
        performRequest(request);
    }

    private void deleteIndex(String index)
    {
        try {
            client.performRequest(new Request("DELETE", "/" + index));
        }
        catch (ResponseException e) {
            // DROP ... IF EXISTS: a missing index is not an error.
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw new UncheckedIOException(e);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void performRequest(Request request)
    {
        try {
            client.performRequest(request);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
