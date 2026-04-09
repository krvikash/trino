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
package io.trino.plugin.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.bson.Document;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.mongodb.MongoQueryRunner.createMongoClient;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

final class TestMongoSamplingDocuments
        extends AbstractTestQueryFramework
{
    private MongoClient client;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        MongoServer server = closeAfterClass(new MongoServer());
        client = closeAfterClass(createMongoClient(server));
        return MongoQueryRunner.builder(server).build();
    }

    @Test
    void testSamplingDefault()
    {
        try (MongoTable table = new MongoTable(client, "test_sampling_default" + randomNameSuffix())) {
            table.insert("{\"id\":1, \"data\":null}");
            table.insert("{\"id\":2, \"data\":true}");

            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES BIGINT '1', 2");
        }
    }

    @Test
    void testSamplingCountWithoutOrder()
    {
        Session samplingCountTwo = Session.builder(getSession())
                .setCatalogSessionProperty("mongodb", "sampling_count", "2")
                .build();

        try (MongoTable table = new MongoTable(client, "test_sampling_default" + randomNameSuffix())) {
            table.insert("{\"id\":1}");
            table.insert("{\"id\":2, \"data\":true}");
            table.insert("{\"id\":3, \"data\":false}");

            // The first document has no "data" field; the second document (within sampling_count=2) introduces data:boolean
            assertThat(query(samplingCountTwo, "SELECT * FROM " + table.name()))
                    .matches("VALUES (BIGINT '1', CAST(null AS boolean)), (2, true), (3, false)");
        }
    }

    @Test
    void testSamplingOrderFirst()
    {
        Session samplingOrderFirst = Session.builder(getSession())
                .setCatalogSessionProperty("mongodb", "sampling_order", "FIRST")
                .build();

        try (MongoTable table = new MongoTable(client, "test_sampling_order_first")) {
            table.insert("{\"id\":1, \"data\":true}");
            table.insert("{\"id\":2}");
            table.insert("{\"id\":3}");

            assertThat(query(samplingOrderFirst, "SELECT * FROM " + table.name()))
                    .matches("VALUES (BIGINT '1', true), (2, NULL), (3, NULL)");
        }
    }

    @Test
    void testSamplingOrderLast()
    {
        Session samplingOrderLast = Session.builder(getSession())
                .setCatalogSessionProperty("mongodb", "sampling_order", "LAST")
                .build();

        try (MongoTable table = new MongoTable(client, "test_sampling_order_last")) {
            table.insert("{\"id\":1}");
            table.insert("{\"id\":2}");
            table.insert("{\"id\":3, \"data\":true}");

            assertThat(query(samplingOrderLast, "SELECT * FROM " + table.name()))
                    .matches("VALUES (BIGINT '1', NULL), (2, NULL), (3, true)");
        }
    }

    @Test
    void testColumnTypeConflict()
    {
        Session samplingCount = Session.builder(getSession())
                .setCatalogSessionProperty("mongodb", "sampling_count", "100")
                .build();

        try (MongoTable table = new MongoTable(client, "test_column_type_conflict")) {
            table.insert("{\"id\":1, \"data\":2147483647}");
            table.insert("{\"id\":2, \"data\":true}");

            assertThat(query(samplingCount, "SELECT * FROM " + table.name())).failure()
                    .hasMessage("Conflicting types for column 'data': bigint and boolean");
        }
    }

    @Test
    void testFieldTypeConflict()
    {
        Session samplingCount = Session.builder(getSession())
                .setCatalogSessionProperty("mongodb", "sampling_count", "100")
                .build();

        try (MongoTable table = new MongoTable(client, "test_field_type_conflict")) {
            table.insert("{\"id\":1, \"data\":{\"f1\":2}}");
            table.insert("{\"id\":2, \"data\":{\"f2\":2}}");

            assertThat(query(samplingCount, "SELECT * FROM " + table.name())).failure()
                    .hasMessage("Conflicting types for column 'data': row(\"f1\" bigint) and row(\"f2\" bigint)");
        }
    }

    @Test
    void testLackOfSampling()
    {
        try (MongoTable table = new MongoTable(client, "test_lack_of_sampling")) {
            table.insert("{\"id\":1, \"data\":null}");
            table.insert("{\"id\":2, \"data\":null}");
            table.insert("{\"id\":3, \"data\":true}");

            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES BIGINT '1', 2, 3");
        }
    }

    @Test
    void testUpdateSchemaProcedureFirstOrder()
    {
        try (MongoTable table = new MongoTable(client, "test_update_schema_first")) {
            table.insert("{\"id\":1}");

            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES BIGINT '1'");

            table.insert("{\"id\":2, \"name\":\"alice\"}");
            table.insert("{\"id\":3, \"name\":\"bob\", \"data\":1.23}");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES BIGINT '1', 2, 3");

            assertUpdate("ALTER TABLE " + table.name() + " EXECUTE update_schema(2, 'FIRST')");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES (BIGINT '1', CAST(NULL AS varchar)), (2, 'alice'), (3, 'bob')");
        }
    }

    @Test
    void testUpdateSchemaProcedureLastOrder()
    {
        try (MongoTable table = new MongoTable(client, "test_update_schema_last")) {
            table.insert("{\"id\":1}");

            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES BIGINT '1'");

            table.insert("{\"id\":2, \"name\":\"alice\"}");
            table.insert("{\"id\":3, \"name\":\"bob\", \"data\":1.23}");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES BIGINT '1', 2, 3");

            assertUpdate("ALTER TABLE " + table.name() + " EXECUTE update_schema(2, 'LAST')");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES (BIGINT '1', CAST(NULL AS varchar), CAST(NULL AS double)), (2, 'alice', NULL), (3, 'bob', 1.23)");
        }
    }

    @Test
    void testUpdateSchemaProcedureTypeChange()
    {
        try (MongoTable table = new MongoTable(client, "test_update_schema_mode")) {
            table.insert("{\"id\":1}");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES BIGINT '1'");

            table.truncate();
            table.insert("{\"id\":\"test id\"}");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES CAST(NULL AS BIGINT)");

            assertQueryFails(
                    "ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST')",
                    "Conflicting types for column 'id': bigint and varchar");
            assertQueryFails(
                    "ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST', 'FAIL')",
                    "Conflicting types for column 'id': bigint and varchar");

            assertUpdate("ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST', 'REPLACE')");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES VARCHAR 'test id'");
        }
    }

    @Test
    void testUpdateSchemaProcedureMissingColumn()
    {
        try (MongoTable table = new MongoTable(client, "test_update_schema_mode")) {
            table.insert("{\"id\":1}");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES BIGINT '1'");

            table.truncate();
            table.insert("{\"name\":\"alice\"}");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES CAST(NULL AS BIGINT)");

            assertQueryFails(
                    "ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST')",
                    "\\QColumns are missing in the new schema: [id]");
            assertQueryFails(
                    "ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST', 'FAIL')",
                    "\\QColumns are missing in the new schema: [id]");

            assertUpdate("ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST', 'REPLACE')");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES VARCHAR 'alice'");
        }
    }

    @Test
    void testUpdateSchemaProcedureWithExistingComment()
    {
        try (MongoTable table = new MongoTable(client, "test_update_schema_comment")) {
            table.insert("{\"id\":1}");

            assertUpdate("COMMENT ON TABLE " + table.name() + " IS 'table comment'");
            assertUpdate("COMMENT ON COLUMN " + table.name() + ".id IS 'column comment'");

            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.name()))
                    .contains("COMMENT 'table comment'")
                    .contains("id bigint COMMENT 'column comment'");

            table.insert("{\"id\":2, \"name\":\"alice\"}");

            assertUpdate("ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST')");
            assertThat((String) computeScalar("SHOW CREATE TABLE " + table.name()))
                    .contains("COMMENT 'table comment'")
                    .contains("id bigint COMMENT 'column comment'");

            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES (BIGINT '1', CAST(NULL AS varchar)), (2, 'alice')");
        }
    }

    @Test
    void testUpdateSchemaProcedureEmptyField()
    {
        try (MongoTable table = new MongoTable(client, "test_update_schema_mode")) {
            table.insert("{\"id\":1}");
            assertThat(query("SELECT * FROM " + table.name()))
                    .matches("VALUES BIGINT '1'");

            table.truncate();

            assertQueryFails("ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST')", ".* has no fields");
            assertQueryFails("ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST', 'REPLACE')", ".* has no fields");
            assertQueryFails("ALTER TABLE " + table.name() + " EXECUTE update_schema(100, 'FIRST', 'FAIL')", ".* has no fields");
        }
    }

    @Test
    void testUpdateSchemaProcedureInvalidArgument()
    {
        try (MongoTable table = new MongoTable(client, "test_update_schema_invalid_argument_")) {
            table.insert("{\"id\":1}");

            assertQueryFails("ALTER TABLE " + table.name() + " EXECUTE update_schema(0, 'FIRST')", ".*sampling_count must be positive: 0");
            assertQueryFails("ALTER TABLE " + table.name() + " EXECUTE update_schema(1, 'INVALID')", ".*Invalid value \\[INVALID]. Valid values: \\[FIRST, LAST]");
            assertQueryFails("ALTER TABLE " + table.name() + " EXECUTE update_schema(1, 'FIRST', 'INVALID')", ".*Invalid value \\[INVALID]. Valid values: \\[REPLACE, FAIL]");
        }
    }

    private static class MongoTable
            implements AutoCloseable
    {
        private final String name;
        private final MongoCollection<Document> collection;

        private MongoTable(MongoClient client, String prefix)
        {
            name = prefix + randomNameSuffix();
            collection = client.getDatabase("tpch").getCollection(name);
        }

        public String name()
        {
            return name;
        }

        public void insert(@Language("JSON") String document)
        {
            collection.insertOne(Document.parse(document));
        }

        public void truncate()
        {
            collection.deleteMany(new Document());
        }

        @Override
        public void close()
        {
            collection.drop();
        }
    }
}
