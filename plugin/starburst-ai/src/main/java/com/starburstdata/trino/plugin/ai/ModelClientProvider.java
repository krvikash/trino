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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.inject.Inject;
import com.starburstdata.trino.plugin.ai.ConnectionInfo.AwsBedrockConnectionInfo;
import com.starburstdata.trino.plugin.ai.ConnectionInfo.OpenAiConnectionInfo;
import com.starburstdata.trino.plugin.ai.bedrock.AwsBedrockClientFactory;
import com.starburstdata.trino.plugin.ai.openai.OpenAiClientFactory;
import io.airlift.slice.Slice;
import io.opentelemetry.api.trace.Tracer;
import io.trino.spi.TrinoException;

import java.util.concurrent.ExecutionException;

import static com.starburstdata.trino.plugin.ai.AiErrorCode.AI_ERROR;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static java.util.Objects.requireNonNull;

public class ModelClientProvider
        implements ClientProvider
{
    private final Tracer tracer;
    private final PromptProvider defaultPromptProvider;
    private final ModelConnectionSpecDao modelConnectionSpecDao;
    private final AwsBedrockClientFactory awsBedrockClientFactory;
    private final OpenAiClientFactory openAiClientFactory;
    private final Cache<Slice, LanguageModelClient> aiClientCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(100));
    private final Cache<Slice, EmbeddingModelClient> embeddingClientCache = buildNonEvictableCache(CacheBuilder.newBuilder().maximumSize(100));

    @Inject
    public ModelClientProvider(
            Tracer tracer,
            PromptProvider defaultPromptProvider,
            ModelConnectionSpecDao modelConnectionSpecDao,
            AwsBedrockClientFactory awsBedrockClientFactory,
            OpenAiClientFactory openAiClientFactory)
    {
        this.tracer = requireNonNull(tracer, "tracer is null");
        this.defaultPromptProvider = requireNonNull(defaultPromptProvider, "defaultPromptProvider is null");
        this.modelConnectionSpecDao = requireNonNull(modelConnectionSpecDao, "modelConnectionSpecDao is null");
        this.awsBedrockClientFactory = requireNonNull(awsBedrockClientFactory, "awsBedrockClientFactory is null");
        this.openAiClientFactory = requireNonNull(openAiClientFactory, "openAiClientFactory is null");
    }

    @Override
    public LanguageModelClient languageModelClient(Slice modelId)
    {
        try {
            return aiClientCache.get(modelId, () -> createLanguageModelClient(modelId));
        }
        catch (ExecutionException e) {
            throw new TrinoException(AI_ERROR, "Failed to create AI client", e);
        }
    }

    private LanguageModelClient createLanguageModelClient(Slice modelId)
    {
        LanguageModelConnectionSpec modelConnectionSpec = modelConnectionSpecDao.getLanguageModelConnectionSpecById(modelId.toStringUtf8());
        PromptProvider promptProvider = new PromptProviderWithOverrides(defaultPromptProvider, modelConnectionSpec.prompts());
        return switch (modelConnectionSpec.connectionInfo()) {
            case OpenAiConnectionInfo openAiConnectionInfo -> openAiClientFactory.createLanguageModelClient(modelConnectionSpec, openAiConnectionInfo, promptProvider, tracer);
            case AwsBedrockConnectionInfo awsBedrockConnectionInfo -> awsBedrockClientFactory.createLanguageModelClient(modelConnectionSpec, awsBedrockConnectionInfo, promptProvider, tracer);
        };
    }

    @Override
    public EmbeddingModelClient embeddingModelClient(Slice modelId)
    {
        try {
            return embeddingClientCache.get(modelId, () -> createEmbeddingModelClient(modelId));
        }
        catch (Exception e) {
            throw new TrinoException(AI_ERROR, "Failed to create AI client", e);
        }
    }

    private EmbeddingModelClient createEmbeddingModelClient(Slice modelId)
    {
        EmbeddingModelConnectionSpec modelConnectionSpec = modelConnectionSpecDao.getEmbeddingModelConnectionSpecById(modelId.toStringUtf8());
        return switch (modelConnectionSpec.connectionInfo()) {
            case ConnectionInfo.OpenAiConnectionInfo openAiConnectionInfo -> openAiClientFactory.createEmbeddingClient(modelConnectionSpec, openAiConnectionInfo);
            case AwsBedrockConnectionInfo awsBedrockConnectionInfo -> awsBedrockClientFactory.createEmbeddingClient(modelConnectionSpec, awsBedrockConnectionInfo);
        };
    }
}
