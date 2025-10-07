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

import io.airlift.units.Duration;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.TEST_AI_SESSION;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

abstract class BaseAiFunctionsSmokeTest
        extends AbstractTestQueryFramework
{
    static final String LANGUAGE_MODEL_ID = "language_model";
    static final String EMBED_MODEL_ID = "embed_model";

    @Test
    void testClassify()
    {
        assertEventually(new Duration(15, SECONDS), new Duration(10, MILLISECONDS), 3, 0.75f, () ->
        {
            String result = (String) computeActual(TEST_AI_SESSION,
                    "SELECT ai.classify('I love this product!', ARRAY['positive', 'negative', 'neutral'], '%s')".formatted(LANGUAGE_MODEL_ID)).getOnlyValue();
            assertThat(result).contains("positive");
        });
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
