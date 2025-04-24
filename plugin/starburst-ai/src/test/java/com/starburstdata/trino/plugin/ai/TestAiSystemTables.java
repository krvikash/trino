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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

import static io.starburst.ai.client.TestingUtils.createModelConnectionSpecsFile;

public class TestAiSystemTables
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File modelsFile = createModelConnectionSpecsFile("""
                {
                 "models": [
                     {
                         "id": "embed1",
                         "modelName": "cohere.embed-multilingual-v3",
                         "kind": "EMBED",
                         "dimensions": 768,
                         "inferenceProfile": "us.cohere.embed-multilingual-v3",
                         "connectionInfo": {
                             "provider": "AWS_BEDROCK",
                             "region": "us-east-1"
                         }
                     },
                     {
                         "id": "embed2",
                         "modelName": "text-embedding-ada-002",
                         "kind": "EMBED",
                         "connectionInfo": {
                             "provider": "OPENAI",
                             "endpoint": "https://api.openai.com/v1",
                             "apiKey": "foo"
                         }
                     },
                     {
                         "id": "language1",
                         "modelName": "gpt-4o-mini",
                         "kind": "GENERATE",
                         "maxTokens": 8192,
                         "temperature": 0.1,
                         "topP": 0.9,
                         "prompts": {
                             "systemPrompts": [
                                 "You are a useful assistant",
                                 "Be funny"
                             ],
                             "analyzeSentimentPrompt": "Analyze sentiment prompt",
                             "analyzeSentimentSystemPrompt": "Analyze sentiment system prompt",
                             "classifyPrompt": "Classify prompt",
                             "classifySystemPrompt": "Classify system prompt",
                             "fixGrammarPrompt": "Fix grammar prompt",
                             "fixGrammarSystemPrompt": "Fix grammar system prompt",
                             "maskPrompt": "Mask prompt",
                             "maskSystemPrompt": "Mask system prompt",
                             "translatePrompt": "Translate prompt",
                             "translateSystemPrompt": "Translate system prompt"
                         },
                         "connectionInfo": {
                             "provider": "OPENAI",
                             "endpoint": "https://api.openai.com/v1",
                             "apiKey": "foo"
                         }
                     },
                     {
                         "id": "language2",
                         "modelName": "meta.llama3-8b-instruct-v1:0",
                         "kind": "GENERATE",
                         "connectionInfo": {
                             "provider": "AWS_BEDROCK",
                             "region": "us-east-1"
                         }
                     }
                 ]
             }
        """);
        return MemoryQueryRunner.builder()
                .setAdditionalSetup(runner -> {
                    runner.installPlugin(new AiPlugin());
                    runner.createCatalog("ai", "starburst_ai", Map.of(
                            "ai.client.models.storage", "FILE",
                            "ai.client.models.file", modelsFile.getAbsolutePath()));
                })
                .build();
    }

    @Test
    public void testLanguageModelsTable()
    {
        assertQuery("""
                    SELECT id, provider, name, endpoint, max_tokens, temperature, top_p, system_prompts,
                    analyze_sentiment_prompt, analyze_sentiment_system_prompt, classify_prompt,
                    classify_system_prompt, fix_grammar_prompt, fix_grammar_system_prompt,
                    mask_prompt, mask_system_prompt, translate_prompt, translate_system_prompt
                    FROM ai.ai.language_models""",
                """
                    VALUES
                    (
                     'language1', 'OPENAI', 'gpt-4o-mini', 'https://api.openai.com/v1', 8192, 0.1, 0.9,
                     ARRAY['You are a useful assistant', 'Be funny'],
                     'Analyze sentiment prompt', 'Analyze sentiment system prompt', 'Classify prompt',
                     'Classify system prompt', 'Fix grammar prompt', 'Fix grammar system prompt',
                     'Mask prompt', 'Mask system prompt', 'Translate prompt', 'Translate system prompt'),
                    (
                     'language2', 'AWS_BEDROCK', 'meta.llama3-8b-instruct-v1:0', NULL, NULL, NULL, NULL, NULL,
                     NULL, NULL, NULL,
                     NULL, NULL, NULL,
                     NULL, NULL, NULL, NULL
                    )
                    """);
    }

    @Test
    public void testLanguageModelsTableMetadata()
    {
        assertQuery("DESCRIBE ai.ai.language_models",
                """
                    VALUES
                    ('id', 'varchar', '', ''),
                    ('provider', 'varchar', '', ''),
                    ('name', 'varchar', '', ''),
                    ('endpoint', 'varchar', '', ''),
                    ('max_tokens', 'integer', '', ''),
                    ('temperature', 'real', '', ''),
                    ('top_p', 'real', '', ''),
                    ('system_prompts', 'array(varchar)', '', ''),
                    ('analyze_sentiment_prompt', 'varchar', '', ''),
                    ('analyze_sentiment_system_prompt', 'varchar', '', ''),
                    ('classify_prompt', 'varchar', '', ''),
                    ('classify_system_prompt', 'varchar', '', ''),
                    ('fix_grammar_prompt', 'varchar', '', ''),
                    ('fix_grammar_system_prompt', 'varchar', '', ''),
                    ('mask_prompt', 'varchar', '', ''),
                    ('mask_system_prompt', 'varchar', '', ''),
                    ('translate_prompt', 'varchar', '', ''),
                    ('translate_system_prompt', 'varchar', '', '')
                    """);
    }

    @Test
    public void testEmbeddingModelsTable()
    {
        assertQuery("""
                    SELECT  id, provider, name, inference_profile, endpoint, dimensions
                    FROM ai.ai.embedding_models""",
                """
                    VALUES
                    ('embed1', 'AWS_BEDROCK', 'cohere.embed-multilingual-v3', 'us.cohere.embed-multilingual-v3', NULL, 768),
                    ('embed2', 'OPENAI', 'text-embedding-ada-002', NULL, 'https://api.openai.com/v1', NULL)
                    """);
    }

    @Test
    public void testEmbeddingModelsTableMetadata()
    {
        assertQuery("DESCRIBE ai.ai.embedding_models",
                """
                    VALUES
                    ('id', 'varchar', '', ''),
                    ('provider', 'varchar', '', ''),
                    ('name', 'varchar', '', ''),
                    ('inference_profile', 'varchar', '', ''),
                    ('endpoint', 'varchar', '', ''),
                    ('dimensions', 'integer', '', '')
                    """);
    }
}
