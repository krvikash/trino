/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.ai.client;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import io.airlift.configuration.secrets.env.EnvironmentVariableSecretProvider;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.json.ObjectMapperProvider;
import io.starburst.ai.client.bedrock.AwsBedrockClientFactory;
import io.starburst.ai.client.bedrock.AwsEmbeddingCodec;
import io.starburst.ai.client.bedrock.CohereEmbedMultilingualV3Codec;
import io.starburst.ai.client.bedrock.TitanTextV2Codec;
import io.starburst.ai.client.openai.OpenAiClientFactory;
import io.starburst.ai.client.openai.oauth.OAuth2TokenCache;
import io.starburst.ai.client.openai.oauth.OAuth2TokenFetcher;
import io.starburst.ai.client.vertexai.VertexAiClientFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public final class TestingUtils
{
    private TestingUtils() {}

    public record TestOperationId(String value)
            implements OperationId {}

    public static final String LANGUAGE_MODEL_PROVIDERS =
            """
            {
                "models": [
                    {
                        "id": "gpt4o_mini",
                        "modelName": "gpt-4o-mini",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1",
                            "apiKey": "${ENV:OPEN_AI_API_KEY}"
                        },
                        "useResponsesApi": "true"
                    },
                    {
                        "id": "gpt4o_mini_auth_header",
                        "modelName": "gpt-4o-mini",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1",
                            "apiKey": "",
                            "additionalHeaders": {"Authorization": ["Bearer ${ENV:OPEN_AI_API_KEY}"]}
                        }
                    },
                    {
                        "id": "meta_llama",
                        "modelName": "us.meta.llama3-3-70b-instruct-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        }
                    },
                    {
                        "id": "haiku45",
                        "modelName": "us.anthropic.claude-haiku-4-5-20251001-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        }
                    },
                    {
                        "id": "haiku45-caching",
                        "modelName": "us.anthropic.claude-haiku-4-5-20251001-v1:0",
                        "kind": "GENERATE",
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        },
                        "traits": {
                            "PROMPT_CACHING_SUPPORT": "PROMPT_CACHING_SUPPORTED"
                        }
                    },
                    {
                        "id": "sonnet45",
                        "modelName": "us.anthropic.claude-sonnet-4-5-20250929-v1:0",
                        "kind": "GENERATE",
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        },
                        "traits": {
                            "PROMPT_CACHING_SUPPORT": "PROMPT_CACHING_SUPPORTED"
                        }
                    },
                    {
                        "id": "mistral_large",
                        "modelName": "mistral.mistral-large-2402-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 2048,
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        }
                    },
                    {
                        "id": "mistral_large_non_streaming",
                        "modelName": "mistral.mistral-large-2402-v1:0",
                        "kind": "GENERATE",
                        "maxTokens": 2048,
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        },
                        "traits": {
                            "STREAMING_TOOL_CALL_SUPPORT": "STREAMING_TOOL_CALL_NOT_SUPPORTED"
                        }
                    },
                    {
                        "id": "reasoning_effort_not_supported",
                        "modelName": "gpt-4o-mini",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1",
                            "apiKey": "${ENV:OPEN_AI_API_KEY}"
                        },
                        "useResponsesApi": "true",
                        "reasoningEffort": "low"
                    },
                    {
                        "id": "openai_error",
                        "modelName": "error",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1",
                            "apiKey": "${ENV:OPEN_AI_API_KEY}"
                        }
                    },
                    {
                        "id": "bedrock_error",
                        "modelName": "error",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.0,
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        }
                    },
                    {
                        "id": "gpt_oss_120b",
                        "modelName": "openai.gpt-oss-120b-1:0",
                        "kind": "GENERATE",
                        "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-1"
                        }
                    }
                ]
            }""";

    public static final String EMBEDDING_MODEL_PROVIDERS =
            """
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
                    "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                    "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                    "region": "us-east-2"
                  }
                },
                {
                  "id": "cohere_3_multi",
                  "modelName": "cohere.embed-multilingual-v3",
                  "kind": "EMBED",
                  "connectionInfo": {
                    "provider": "AWS_BEDROCK",
                    "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                    "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                    "region": "us-east-1"
                  }
                }
              ]
            }""";

    public static ModelClientProvider staticModelClientProvider(String modelSpecJson, ScheduledExecutorService reloadingExecutor, ExecutorService llmExecutor)
    {
        return staticModelClientProvider(modelSpecJson, reloadingExecutor, llmExecutor, TokenUsageListener.NOOP);
    }

    public static ModelClientProvider staticModelClientProvider(String modelSpecJson, ScheduledExecutorService reloadingExecutor, ExecutorService llmExecutor, TokenUsageListener tokenUsageListener)
    {
        File modelsFile = createModelConnectionSpecsFile(modelSpecJson);
        return createModelClientProvider(modelsFile, false, reloadingExecutor, llmExecutor, tokenUsageListener);
    }

    public static ReloadingModelClientProvider reloadingModelClientProvider(File modelsFile, ScheduledExecutorService reloadingExecutor, ExecutorService llmExecutor)
    {
        ReloadingModelClientProvider reloadingModelClientProvider =
                createModelClientProvider(modelsFile, true, reloadingExecutor, llmExecutor, TokenUsageListener.NOOP);
        reloadingModelClientProvider.start();
        return reloadingModelClientProvider;
    }

    private static ReloadingModelClientProvider createModelClientProvider(
            File modelsFile,
            boolean clientCacheRefreshEnabled,
            ScheduledExecutorService reloadingExecutor,
            ExecutorService llmExecutor,
            TokenUsageListener tokenUsageListener)
    {
        AiFileStorageConfig config = new AiFileStorageConfig().setModelConnectionSpecsFile(modelsFile.getAbsolutePath());
        FileBackedModelConnectionSpecsLoader modelSpecsLoader = new FileBackedModelConnectionSpecsLoader(config);

        PromptDao promptDao = new StaticPromptDao();
        Map<String, AwsEmbeddingCodec.Factory> awsEmbeddingCodecFactories = ImmutableMap.<String, AwsEmbeddingCodec.Factory>builder()
                .put(TitanTextV2Codec.MODEL_NAME, new TitanTextV2Codec.Factory())
                .put(CohereEmbedMultilingualV3Codec.MODEL_NAME, new CohereEmbedMultilingualV3Codec.Factory())
                .buildOrThrow();

        SecretsResolver secretsResolver = new SecretsResolver(ImmutableMap.of("env", new EnvironmentVariableSecretProvider()));
        AiClientConfig aiClientConfig = new AiClientConfig();
        AwsBedrockClientFactory bedrockClientFactory = new AwsBedrockClientFactory(awsEmbeddingCodecFactories, secretsResolver, aiClientConfig, llmExecutor);
        OAuth2TokenCache oauth2TokenCache = createTestOAuth2TokenCache(aiClientConfig);
        OpenAiClientFactory openAiClientFactory = new OpenAiClientFactory(secretsResolver, aiClientConfig, llmExecutor, new ObjectMapperProvider().get(), oauth2TokenCache);
        VertexAiClientFactory vertexAiClientFactory = new VertexAiClientFactory(secretsResolver, aiClientConfig, llmExecutor);
        return new ReloadingModelClientProvider(
                promptDao,
                modelSpecsLoader,
                bedrockClientFactory,
                openAiClientFactory,
                vertexAiClientFactory,
                oauth2TokenCache,
                new AiClientConfig()
                        .setClientCacheRefreshEnabled(clientCacheRefreshEnabled),
                secretsResolver,
                reloadingExecutor,
                tokenUsageListener);
    }

    public static OAuth2TokenCache createTestOAuth2TokenCache(AiClientConfig config)
    {
        HttpClient httpClient = new TestingHttpClient(_ -> {
            throw new UnsupportedOperationException("OAuth2 HTTP client not configured for this test");
        });
        return new OAuth2TokenCache(new OAuth2TokenFetcher(httpClient), config);
    }

    public static File createModelConnectionSpecsFile(String content)
    {
        try {
            File tempFile = File.createTempFile("model_connection_specs_", ".json");
            tempFile.deleteOnExit();
            try (Writer writer = Files.newBufferedWriter(tempFile.toPath())) {
                writer.write(content);
            }
            return tempFile;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static ExecutorService createLlmExecutor()
    {
        return newCachedThreadPool(daemonThreadsNamed("llm-invoker-executor-%s"));
    }
}
