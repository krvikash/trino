/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.functions.ai;

import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.TEST_AI_SESSION;
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.addStarburstAiCatalog;
import static io.starburst.ai.client.TestingUtils.EMBEDDING_MODEL_PROVIDERS;

public class TestGenerateEmbeddings
        extends AbstractTestQueryFramework
{
    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        return MemoryQueryRunner.builder()
                .setAdditionalSetup(runner -> addStarburstAiCatalog(EMBEDDING_MODEL_PROVIDERS, runner))
                .build();
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testGenerateEmbeddings(String modelId)
    {
        try (TestTable table = newTrinoTable(
                "test_generate_embeddings_",
                "(data VARCHAR, embedding ARRAY(DOUBLE))")) {
            assertUpdate(
                    """
                    INSERT INTO %s VALUES
                    ('apple', starburst.ai.generate_embedding('apple', '%2$s')),
                    ('orange', starburst.ai.generate_embedding('orange', '%2$s')),
                    ('cat', starburst.ai.generate_embedding('cat', '%2$s')),
                    ('dog', starburst.ai.generate_embedding('dog', '%2$s')),
                    ('Some text with a " few \n \t \\ \r \b special \f \0 characters', starburst.ai.generate_embedding('Some text with a " few \n \t \\ \r \b special \f \0 characters', '%2$s')),
                    ('shirt', starburst.ai.generate_embedding('shirt', '%2$s')),
                    ('pants', starburst.ai.generate_embedding('pants', '%2$s'))
                    """.formatted(table.getName(), modelId),
                    7);

            assertQuery(
                    "SELECT data FROM (SELECT data, cosine_similarity(embedding, starburst.ai.generate_embedding('animal', '%2$s')) AS similarity FROM %1$s ORDER BY similarity DESC LIMIT 2)".formatted(table.getName(), modelId),
                    "VALUES 'cat', 'dog'");
        }
    }

    @ParameterizedTest
    @MethodSource("modelIds")
    public void testGenerateEmbeddingsTableFunction(String modelId)
    {
        try (TestTable table = newTrinoTable(
                "test_generate_embeddings_table_function_",
                "(id INT, data VARCHAR, embedding ARRAY(REAL))")) {
            assertUpdate(
                    """
                    INSERT INTO %s (id, data, embedding)
                    SELECT id, data, embedding
                    FROM TABLE(starburst.ai.generate_embeddings(
                      embedding_column => DESCRIPTOR(embedding),
                      data_column => DESCRIPTOR(data),
                      source => TABLE(SELECT * FROM (VALUES (0, 'apple'), (1, 'orange'), (2, null), (3, ''), (4, 'cat'), (5, 'dog'), (6, 'shirt'), (7, 'pants')) AS t (id, data)),
                      model_id => '%s'))
                    """.formatted(table.getName(), modelId),
                    8);

            assertQuery(
                    "SELECT id, data FROM (SELECT id, data, cosine_similarity(embedding, starburst.ai.generate_embedding('animal', '%2$s')) AS similarity FROM %1$s ORDER BY similarity DESC LIMIT 2)".formatted(table.getName(), modelId),
                    "VALUES (4, 'cat'), (5, 'dog')");
        }

        try (TestTable table = newTrinoTable(
                "test_generate_embeddings_table_function_",
                "(data VARCHAR, embedding ARRAY(DOUBLE))")) {
            assertUpdate(
                    """
                    INSERT INTO %s (data, embedding)
                    SELECT data, embedding
                    FROM TABLE(starburst.ai.generate_embeddings(
                      embedding_column => DESCRIPTOR(embedding),
                      data_column => DESCRIPTOR(data),
                      source => TABLE(SELECT * FROM (VALUES 'apple', 'orange', null, '', 'cat', 'dog', 'shirt', 'pants') AS t (data)),
                      model_id => '%s'))
                    """.formatted(table.getName(), modelId),
                    8);

            assertQuery(
                    "SELECT data FROM (SELECT data, cosine_similarity(embedding, starburst.ai.generate_embedding('animal', '%2$s')) AS similarity FROM %1$s ORDER BY similarity DESC LIMIT 2)".formatted(table.getName(), modelId),
                    "VALUES 'cat', 'dog'");
        }
    }

    @Test
    public void testValidations()
    {
        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  embedding_column => DESCRIPTOR(embeddings VARCHAR),
                  data_column => DESCRIPTOR(data),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => 'openai_embed_3_small'))
                """,
                "EMBEDDING_COLUMN descriptor references an unsupported type: varchar");
        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  embedding_column => DESCRIPTOR(embeddings, data),
                  data_column => DESCRIPTOR(data),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => 'openai_embed_3_small'))
                """,
                "EMBEDDING_COLUMN descriptor contains more than one column");
        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  data_column => DESCRIPTOR(data),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => 'openai_embed_3_small'))
                """,
                ".*Missing argument: EMBEDDING_COLUMN");

        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  embedding_column => DESCRIPTOR(embeddings),
                  data_column => DESCRIPTOR(data INT),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => 'openai_embed_3_small'))
                """,
                "DATA_COLUMN descriptor contains types");
        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  embedding_column => DESCRIPTOR(embeddings),
                  data_column => DESCRIPTOR(data, embeddings),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => 'openai_embed_3_small'))
                """,
                "DATA_COLUMN descriptor contains more than one column");
        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  embedding_column => DESCRIPTOR(embeddings),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => 'openai_embed_3_small'))
                """,
                ".*Missing argument: DATA_COLUMN");

        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  embedding_column => DESCRIPTOR(embeddings),
                  data_column => DESCRIPTOR(data),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => null))
                """,
                "MODEL_ID value cannot be null");
        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  embedding_column => DESCRIPTOR(embeddings),
                  data_column => DESCRIPTOR(data),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => ''))
                """,
                "MODEL_ID value cannot be empty");

        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  embedding_column => DESCRIPTOR(data),
                  data_column => DESCRIPTOR(data),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => 'openai_embed_3_small'))
                """,
                "Embedding column must not be present in SOURCE input");
        assertQueryFails(
                """
                SELECT * FROM  TABLE(starburst.ai.generate_embeddings(
                  embedding_column => DESCRIPTOR(embedding),
                  data_column => DESCRIPTOR(not_here),
                  source => TABLE(SELECT * FROM (VALUES 'apple', 'orange') AS t (data)),
                  model_id => 'openai_embed_3_small'))
                """,
                "Column not_here not present in the table");
    }

    @ParameterizedTest
    @MethodSource("supportsBinaryEmbeddings")
    public void testGenerateEmbeddingsTableFunctionWithVarbinaryEncoding(String modelId)
    {
        try (TestTable table = newTrinoTable(
                "test_generate_embeddings_table_function_",
                "(data VARCHAR, embedding VARBINARY)")) {
            assertUpdate(
                    """
                    INSERT INTO %s (data, embedding)
                    SELECT data, embedding
                    FROM TABLE(starburst.ai.generate_embeddings(
                      embedding_column => DESCRIPTOR(embedding VARBINARY),
                      data_column => DESCRIPTOR(data),
                      source => TABLE(SELECT * FROM (VALUES 'apple', 'orange', null, '', 'cat', 'dog', 'shirt', 'pants') AS t (data)),
                      model_id => '%s'))
                    """.formatted(table.getName(), modelId),
                    8);

            assertQuery(
                    ("SELECT data FROM (SELECT data, hamming_distance(embedding, starburst.ai.generate_binary_embedding('clothing', '%2$s')) AS distance FROM %1$s " +
                            "ORDER BY distance ASC LIMIT 2)").formatted(table.getName(), modelId),
                    "VALUES 'shirt', 'pants'");
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

    public static Object[][] supportsBinaryEmbeddings()
    {
        return new Object[][] {
                {"titan_v2"},
                {"cohere_3_multi"},
        };
    }
}
