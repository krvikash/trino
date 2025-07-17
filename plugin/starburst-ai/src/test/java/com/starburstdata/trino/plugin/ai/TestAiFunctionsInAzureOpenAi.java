/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.ai;

import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static com.starburstdata.trino.plugin.ai.AiQueryRunner.TEST_AI_SESSION;
import static com.starburstdata.trino.plugin.ai.AiQueryRunner.addStarburstAiCatalog;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static org.assertj.core.api.Assertions.assertThat;

class TestAiFunctionsInAzureOpenAi
        extends AbstractTestQueryFramework
{
    private static final String LANGUAGE_MODEL_ID = "gpt4o_mini";
    private static final String EMBED_MODEL_ID = "openai_embed_3_small";

    private static final String AZURE_OPEN_AI_LANGUAGE_ENDPOINT = requireEnv("AZURE_OPEN_AI_LANGUAGE_ENDPOINT");
    private static final String AZURE_OPEN_AI_EMBED_ENDPOINT = requireEnv("AZURE_OPEN_AI_EMBED_ENDPOINT");

    private static final String AZURE_OPEN_AI_MODEL_PROVIDERS = """
                {
                  "models": [
                     {
                        "id": "%s",
                        "modelName": "gpt-4o-mini",
                        "kind": "GENERATE",
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "%s",
                            "apiKey": "${ENV:AZURE_OPEN_AI_API_KEY}"
                        }
                    },
                    {
                      "id": "%s",
                      "modelName": "text-embedding-3-small",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "OPENAI",
                        "endpoint": "%s",
                        "apiKey": "${ENV:AZURE_OPEN_AI_API_KEY}"
                      }
                    }
                  ]
                }""".formatted(LANGUAGE_MODEL_ID, AZURE_OPEN_AI_LANGUAGE_ENDPOINT, EMBED_MODEL_ID, AZURE_OPEN_AI_EMBED_ENDPOINT);

    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setAdditionalSetup(runner -> addStarburstAiCatalog(AZURE_OPEN_AI_MODEL_PROVIDERS, runner))
                .build();
    }

    @Test
    void testClassify()
    {
        String result = (String) computeActual(TEST_AI_SESSION,
                "SELECT ai.classify('I love this product!', ARRAY['positive', 'negative', 'neutral'], '%s')".formatted(LANGUAGE_MODEL_ID)).getOnlyValue();
        assertThat(result).contains("positive");
    }

    @Test
    void testGenerateEmbeddingsTableFunction()
    {
        try (TestTable table = newTrinoTable(
                "test_generate_embeddings_table_function_",
                "(id INT, data VARCHAR, embedding ARRAY(REAL))")) {
            assertUpdate("""
                    INSERT INTO %s (id, data, embedding)
                    SELECT id, data, embedding
                    FROM TABLE(starburst.ai.generate_embeddings(
                      embedding_column => DESCRIPTOR(embedding),
                      data_column => DESCRIPTOR(data),
                      source => TABLE(SELECT * FROM (VALUES (0, 'apple'), (1, 'orange'), (2, null), (3, ''), (4, 'cat'), (5, 'dog'), (6, 'shirt'), (7, 'pants')) AS t (id, data)),
                      model_id => '%s'))
                    """.formatted(table.getName(), EMBED_MODEL_ID), 8);

            assertQuery(
                    "SELECT id, data FROM (SELECT id, data, cosine_similarity(embedding, starburst.ai.generate_embedding('animal', '%2$s')) AS similarity FROM %1$s ORDER BY similarity DESC LIMIT 2)".formatted(table.getName(), EMBED_MODEL_ID),
                    "VALUES (4, 'cat'), (5, 'dog')");
        }
    }
}
