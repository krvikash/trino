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
import io.airlift.tracing.Tracing;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.bedrock.AwsBedrockClientFactory;
import io.starburst.ai.client.bedrock.AwsEmbeddingCodec;
import io.starburst.ai.client.bedrock.CohereEmbedMultilingualV3Codec;
import io.starburst.ai.client.bedrock.TitanTextV2Codec;
import io.starburst.ai.client.openai.OpenAiClientFactory;

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

public class TestingUtils
{
    private TestingUtils() {}

    public static final String LANGUAGE_MODEL_PROVIDERS = """
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
                        "id": "gpt4o_mini_auth_header",
                        "modelName": "gpt-4o-mini",
                        "kind": "GENERATE",
                        "maxTokens": 8192,
                        "temperature": 0.1,
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1",
                            "apiKey": "",
                            "additionalHeaders": {"Authorization": ["bearer ${ENV:OPEN_AI_API_KEY}"]}
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
                            "awsAccessKey": "${ENV:AWS_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:AWS_SECRET_ACCESS_KEY}",
                            "region": "us-east-2"
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
                            "awsAccessKey": "${ENV:AWS_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:AWS_SECRET_ACCESS_KEY}",
                            "region": "us-east-2"
                        }
                    }
                ]
            }""";

    public static final String EMBEDDING_MODEL_PROVIDERS = """
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

    public static ModelClientProvider staticModelClientProvider(String modelSpecJson, ScheduledExecutorService reloadingExecutor, ExecutorService llmExecutor)
    {
        File modelsFile = createModelConnectionSpecsFile(modelSpecJson);
        return createModelClientProvider(modelsFile, false, reloadingExecutor, llmExecutor);
    }

    public static ReloadingModelClientProvider reloadingModelClientProvider(File modelsFile, ScheduledExecutorService reloadingExecutor, ExecutorService llmExecutor)
    {
        ReloadingModelClientProvider reloadingModelClientProvider = createModelClientProvider(modelsFile, true, reloadingExecutor, llmExecutor);
        reloadingModelClientProvider.start();
        return reloadingModelClientProvider;
    }

    private static ReloadingModelClientProvider createModelClientProvider(File modelsFile, boolean clientCacheRefreshEnabled, ScheduledExecutorService reloadingExecutor, ExecutorService llmExecutor)
    {
        AiFileStorageConfig config = new AiFileStorageConfig().setModelConnectionSpecsFile(modelsFile.getAbsolutePath());
        FileBackedModelConnectionSpecsLoader modelSpecsLoader = new FileBackedModelConnectionSpecsLoader(config);

        PromptDao promptDao = new StaticPromptDao();
        Tracer tracer = Tracing.noopTracer();
        Map<String, AwsEmbeddingCodec.Factory> awsEmbeddingCodecFactories = ImmutableMap.<String, AwsEmbeddingCodec.Factory>builder()
                .put(TitanTextV2Codec.MODEL_NAME, new TitanTextV2Codec.Factory())
                .put(CohereEmbedMultilingualV3Codec.MODEL_NAME, new CohereEmbedMultilingualV3Codec.Factory())
                .buildOrThrow();

        SecretsResolver secretsResolver = new SecretsResolver(ImmutableMap.of("env", new EnvironmentVariableSecretProvider()));
        AiClientConfig aiClientConfig = new AiClientConfig();
        AwsBedrockClientFactory bedrockClientFactory = new AwsBedrockClientFactory(awsEmbeddingCodecFactories, secretsResolver, aiClientConfig, llmExecutor);
        OpenAiClientFactory openAiClientFactory = new OpenAiClientFactory(secretsResolver, aiClientConfig, llmExecutor);
        return new ReloadingModelClientProvider(
                tracer,
                promptDao,
                modelSpecsLoader,
                bedrockClientFactory,
                openAiClientFactory,
                new AiClientConfig()
                        .setClientCacheRefreshEnabled(clientCacheRefreshEnabled),
                secretsResolver,
                reloadingExecutor);
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
