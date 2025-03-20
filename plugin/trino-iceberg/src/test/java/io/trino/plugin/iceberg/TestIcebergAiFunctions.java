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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import com.starburstdata.trino.plugin.ai.AiPlugin;
import com.starburstdata.trino.plugin.ai.TestingUtils;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergAiFunctions
        extends AbstractTestQueryFramework
{
    private static final String MODEL_PROVIDERS = """
        {
            "models": [
                {
                  "id": "cohere",
                  "modelName": "cohere.embed-multilingual-v3",
                  "kind": "EMBED",
                  "connectionInfo": {
                    "provider": "AWS_BEDROCK",
                    "region": "us-east-1"
                  }
                },
                {
                    "id": "openai",
                    "modelName": "text-embedding-3-small",
                    "kind": "EMBED",
                    "connectionInfo": {
                        "provider": "OPENAI",
                        "endpoint": "https://api.openai.com/v1",
                        "apiKey": "${ENV:OPEN_AI_API_KEY}"
                    }
                }
            ]
        }
        """;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File modelsFile = TestingUtils.createModelConnectionSpecsFile(MODEL_PROVIDERS);
        return IcebergQueryRunner.builder()
                .setIcebergProperties(ImmutableMap.of(
                        "ai.models-file", modelsFile.getAbsolutePath()))
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new AiPlugin());
                    runner.createCatalog("ai", "starburst_ai", Map.of("ai.models-file", modelsFile.getAbsolutePath()));
                })
                .build();
    }

    @Test
    public void testGenerateEmbeddingsProcedure()
    {
        try (TestTable table = newTrinoTable("test_generate_embeddings_procedure", "(data VARCHAR)")) {
            assertUpdate("INSERT INTO %s VALUES 'apple', 'orange', 'cat', 'dog', null, '', 'shirt', 'pants'".formatted(table.getName()), 8);
            assertUpdate("ALTER TABLE %s ADD COLUMN embedding ARRAY(DOUBLE)".formatted(table.getName()));
            assertUpdate("ALTER TABLE %s EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'openai')".formatted(table.getName()));

            assertQuery(
                    "SELECT data FROM (SELECT data, cosine_similarity(embedding, ai.ai.generate_embedding('animal', 'openai')) AS similarity FROM %s ORDER BY similarity DESC LIMIT 2)".formatted(table.getName()),
                    "VALUES 'cat', 'dog'");
        }

        try (TestTable table = newTrinoTable("test_generate_embeddings_procedure", "(data VARCHAR)")) {
            assertUpdate("INSERT INTO %s VALUES 'apple', 'orange', 'cat', 'dog', null, '', 'shirt', 'pants'".formatted(table.getName()), 8);
            assertUpdate("ALTER TABLE %s ADD COLUMN embedding ARRAY(REAL)".formatted(table.getName()));
            assertUpdate("ALTER TABLE %s EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'openai')".formatted(table.getName()));

            assertQuery(
                    "SELECT data FROM (SELECT data, cosine_similarity(embedding, ai.ai.generate_embedding('animal', 'openai')) AS similarity FROM %s ORDER BY similarity DESC LIMIT 2)".formatted(table.getName()),
                    "VALUES 'cat', 'dog'");
        }

        try (TestTable table = newTrinoTable(
                "test_generate_embeddings_procedure_partitioned_table_",
                "(c1 INT, data VARCHAR) WITH (partitioning = ARRAY['c1', 'data'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'apple'), (2, 'orange'), (3, 'cat'), (4, 'dog'), (5, null), (6, ''), (7, 'shirt'), (8, 'pants')",
                    8);
            // Create embedding for partition column
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN embedding ARRAY(DOUBLE)");
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'openai')");
            assertQuery(
                    "SELECT data FROM (SELECT data, cosine_similarity(embedding, ai.ai.generate_embedding('animal', 'openai')) AS similarity FROM " 
                            + table.getName() 
                            + " ORDER BY similarity DESC LIMIT 2)",
                    "VALUES 'cat', 'dog'");
            // Conditionally create embeddings
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN data_orange_embedding ARRAY(DOUBLE)");
            assertUpdate("ALTER TABLE " 
                    + table.getName() 
                    + " EXECUTE generate_embeddings(embedding_column => 'data_orange_embedding', data_column => 'data', model_id => 'openai') WHERE c1 = 2");
            assertThat(query("SELECT c1 FROM " + table.getName() + " WHERE data_orange_embedding IS NOT NULL"))
                    .matches("VALUES 2");
        }
    }

    @Test
    public void testBinaryEmbeddings()
    {
        try (TestTable table = newTrinoTable("test_binary_embeddings_", "(data VARCHAR)")) {
            assertUpdate("INSERT INTO %s VALUES 'apple', 'orange', 'cat', 'dog', null, '', 'shirt', 'pants'".formatted(table.getName()), 8);
            assertUpdate("ALTER TABLE %s ADD COLUMN embedding VARBINARY".formatted(table.getName()));
            assertUpdate("ALTER TABLE %s EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'cohere')".formatted(table.getName()));

            assertQuery(
                    ("SELECT data FROM (SELECT data, hamming_distance(embedding, ai.ai.generate_binary_embedding('clothing', 'cohere')) AS distance " +
                            "FROM %s ORDER BY distance ASC LIMIT 2)").formatted(table.getName()),
                    "VALUES 'shirt', 'pants'");
        }
    }
}
