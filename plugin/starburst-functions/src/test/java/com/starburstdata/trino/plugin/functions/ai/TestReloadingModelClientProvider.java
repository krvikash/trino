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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;

import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.TEST_AI_SESSION;
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.addStarburstAiCatalog;
import static io.starburst.ai.client.TestingUtils.createModelConnectionSpecsFile;
import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestReloadingModelClientProvider
        extends AbstractTestQueryFramework
{
    private static final String MODEL_SPECS_V1 =
            """
            {
                "models": [
                    {
                        "id": "gpt4o_mini",
                        "modelName": "gpt-4o-mini",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1",
                            "apiKey": "${ENV:OPEN_AI_API_KEY}"
                        }
                    },
                    {
                        "id": "meta_llama",
                        "modelName": "us.meta.llama3-3-70b-instruct-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-2"
                        }
                    },
                    {
                      "id": "titan_v2",
                      "modelName": "amazon.titan-embed-text-v2:0",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "AWS_BEDROCK",
                        "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                        "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                        "region": "us-east-2"
                      }
                    }
                ]
            }""";

    String MODEL_SPECS_V2 =
            """
            {
                "models": [
                    {
                        "id": "meta_llama",
                        "modelName": "us.meta.llama3-3-70b-instruct-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-2"
                        },
                        "prompts": {
                            "systemPrompts": [
                              "If asked to return the capital of France, please respond with Paname and not Paris",
                              "If asked to mask values, only substitute the values that are masked, never substitute [MASKED] for the labels."
                            ],
                            "classifyPrompt": "Mask the values for each of the JSON encoded labels in the text below.\\nLabels: %s\\nReplace the values with the text \\"[MASKED]\\".\\nOutput only the masked text.\\nDo not output anything else.\\n=====\\n%s\\n"
                        }
                    },
                    {
                        "id": "haiku35",
                        "modelName": "us.anthropic.claude-3-5-haiku-20241022-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-2"
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
                    }
                ]
            }""";

    private File modelSpecsFile;

    @Override
    public QueryRunner createQueryRunner()
            throws Exception
    {
        this.modelSpecsFile = createModelConnectionSpecsFile(MODEL_SPECS_V1);
        return MemoryQueryRunner.builder()
                .setAdditionalSetup(runner -> addStarburstAiCatalog(modelSpecsFile, runner))
                .build();
    }

    @Test
    public void testRefreshingModelClientProvider()
            throws IOException
    {
        assertThat(simplePrompt("meta_llama")).isEqualTo("paris");
        assertThat(simplePrompt("gpt4o_mini")).isEqualTo("paris");
        assertThatThrownBy(() -> simplePrompt("haiku35"))
                .hasMessage("Language model client not found for id: haiku35");

        String result = (String) computeActual(
                TEST_AI_SESSION,
                "SELECT ai.classify('I love this product!', ARRAY['positive', 'negative', 'neutral'], '%s')".formatted("meta_llama")).getOnlyValue();
        assertThat(result).contains("positive");
        assertThat(simpleEmbedding("titan_v2"))
                .isNotEmpty()
                .allMatch(Objects::nonNull);
        assertThatThrownBy(() -> simpleEmbedding("openai_embed_3_large"))
                .hasMessage("Embedding model client not found for id: openai_embed_3_large");
        Files.writeString(modelSpecsFile.toPath(), MODEL_SPECS_V2);
        assertEventually(() -> assertThat(simplePrompt("haiku35")).isEqualTo("paris"));
        assertEventually(() -> assertThat(simplePrompt("meta_llama")).isEqualTo("paname"));
        assertEventually(() -> assertThatThrownBy(() -> simplePrompt("gpt4o_mini"))
                .hasMessage("Language model client not found for id: gpt4o_mini"));
        // Assert that the system prompt change was picked up
        assertEventually(() -> assertThat(simpleEmbedding("openai_embed_3_large"))
                .isNotEmpty()
                .allMatch(Objects::nonNull));
        assertEventually(() -> assertThatThrownBy(() -> simpleEmbedding("titan_v2"))
                .hasMessage("Embedding model client not found for id: titan_v2"));
        // Verify that the updated classify prompt is set to the mask prompt.
        String prompt = "My credit card number is 1234-5678-9012-3456 and my password is hunter2";
        assertEventually(() -> {
            String modifiedPromptResult = (String) computeActual(
                    TEST_AI_SESSION,
                    "SELECT ai.classify('%s', ARRAY['credit card number', 'password'], '%s')".formatted(prompt, "meta_llama")).getOnlyValue();
            assertThat(modifiedPromptResult.strip())
                    .isEqualTo("My credit card number is [MASKED] and my password is [MASKED]");
        });
    }

    private String simplePrompt(String modelId)
    {
        String prompt = "What is the capital of France? Only return the name of the city and no extraneous text.";
        return ((String) computeActual(TEST_AI_SESSION, "SELECT ai.prompt('%s', '%s')".formatted(prompt, modelId)).getOnlyValue()).toLowerCase(ENGLISH).strip();
    }

    @SuppressWarnings("unchecked")
    private List<Double> simpleEmbedding(String modelId)
    {
        return (List<Double>) computeActual(TEST_AI_SESSION, "SELECT ai.generate_embedding('apple', '%s')".formatted(modelId)).getOnlyValue();
    }
}
