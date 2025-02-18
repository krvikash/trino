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
package com.starburstdata.trino.plugin.ai;

import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Map;

import static com.starburstdata.trino.plugin.ai.TestingUtils.TEST_AI_SESSION;
import static com.starburstdata.trino.plugin.ai.TestingUtils.createModelConnectionSpecsFile;

public class TestGenerateEmbeddings
        extends AbstractTestQueryFramework
{
    private static final String MODEL_PROVIDERS = """
                {
                  "models": [
                    {
                      "id": "openai_embed_3_small",
                      "modelName": "text-embedding-3-small",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "OPENAI",
                        "endpoint": "https://api.openai.com/v1",
                        "apiKey": "${ENV:OPEN_AI_API_KEY}"
                      }
                    },
                    {
                      "id": "openai_embed_3_large",
                      "modelName": "text-embedding-3-large",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "OPENAI",
                        "endpoint": "https://api.openai.com/v1",
                        "apiKey": "${ENV:OPEN_AI_API_KEY}"
                      }
                    },
                    {
                      "id": "titan_v2",
                      "modelName": "amazon.titan-embed-text-v2:0",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "AWS_BEDROCK",
                        "awsAccessKey": "${ENV:AWS_ACCESS_KEY_ID}",
                        "awsSecretKey": "${ENV:AWS_SECRET_ACCESS_KEY}",
                        "region": "us-east-2"
                      }
                    },
                    {
                      "id": "cohere_3_multi",
                      "modelName": "cohere.embed-multilingual-v3",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "AWS_BEDROCK",
                        "awsAccessKey": "${ENV:AWS_ACCESS_KEY_ID}",
                        "awsSecretKey": "${ENV:AWS_SECRET_ACCESS_KEY}",
                        "region": "us-east-1"
                      }
                    }
                  ]
                }""";

    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        File modelsFile = createModelConnectionSpecsFile(MODEL_PROVIDERS);
        return MemoryQueryRunner.builder()
                .addCoordinatorProperty("sql.path", "ai")
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new AiPlugin());
                    runner.createCatalog("ai", "starburst_ai", Map.of(
                            "ai.models-file", modelsFile.getAbsolutePath()));
                })
                .build();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testGenerateEmbeddings(String modelId)
    {
        try (TestTable table = newTrinoTable(
                "test_generate_embeddings_",
                "(data VARCHAR, embedding ARRAY(DOUBLE))")) {
            assertUpdate("""
                            INSERT INTO %s VALUES
                            ('apple', ai.ai.generate_embedding('apple', '%2$s')),
                            ('orange', ai.ai.generate_embedding('orange', '%2$s')),
                            ('cat', ai.ai.generate_embedding('cat', '%2$s')),
                            ('dog', ai.ai.generate_embedding('dog', '%2$s')),
                            ('Some text with a " few \n \t \\ \r \b special \f \0 characters', ai.ai.generate_embedding('Some text with a " few \n \t \\ \r \b special \f \0 characters', '%2$s')),
                            ('shirt', ai.ai.generate_embedding('shirt', '%2$s')),
                            ('pants', ai.ai.generate_embedding('pants', '%2$s'))
                            """.formatted(table.getName(), modelId),
                    7);

            assertQuery(
                    "SELECT data FROM (SELECT data, cosine_similarity(embedding, ai.ai.generate_embedding('animal', '%2$s')) AS similarity FROM %1$s ORDER BY similarity DESC LIMIT 2)".formatted(table.getName(), modelId),
                    "VALUES 'cat', 'dog'");
        }
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testGenerateEmbeddingsWithEmptyString(String modelId)
    {
        assertQuery(TEST_AI_SESSION, "SELECT ai.generate_embedding('', '%s')".formatted(modelId), "VALUES NULL");
    }

    public static Object[][] modelIds()
    {
        return new Object[][] {
                {"openai_embed_3_small"},
                {"openai_embed_3_large"},
                {"titan_v2"},
                {"cohere_3_multi"},
        };
    }
}
