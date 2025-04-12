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
package io.starburst.ai.client.openai;

import com.openai.client.OpenAIClient;
import com.openai.client.okhttp.OpenAIOkHttpClient;
import io.opentelemetry.api.trace.Tracer;
import io.starburst.ai.client.ConnectionInfo.OpenAiConnectionInfo;
import io.starburst.ai.client.EmbeddingModelClient;
import io.starburst.ai.client.EmbeddingModelConnectionSpec;
import io.starburst.ai.client.LanguageModelClient;
import io.starburst.ai.client.LanguageModelConnectionSpec;
import io.starburst.ai.client.ModelClientFactory;
import io.starburst.ai.client.PromptDao;

import static java.util.Objects.requireNonNull;

public class OpenAiClientFactory
        implements ModelClientFactory<OpenAiConnectionInfo>
{
    @Override
    public LanguageModelClient createLanguageModelClient(LanguageModelConnectionSpec spec, OpenAiConnectionInfo connectionInfo, PromptDao promptDao, Tracer tracer)
    {
        requireNonNull(spec, "spec is null");
        requireNonNull(connectionInfo, "connectionInfo is null");
        return new OpenAiLanguageModelClient(
                spec.modelName(),
                spec.temperature(),
                spec.maxTokens(),
                spec.topP(),
                spec.useDeveloperForSystemRole(),
                promptDao,
                tracer,
                createOpenAiClient(connectionInfo));
    }

    @Override
    public EmbeddingModelClient createEmbeddingClient(EmbeddingModelConnectionSpec spec, OpenAiConnectionInfo connectionInfo)
    {
        requireNonNull(connectionInfo, "connectionInfo is null");
        return new OpenAiEmbeddingModelClient(spec, createOpenAiClient(connectionInfo));
    }

    private static OpenAIClient createOpenAiClient(OpenAiConnectionInfo connectionInfo)
    {
        OpenAIOkHttpClient.Builder builder = OpenAIOkHttpClient.builder();
        connectionInfo.apiKey().ifPresent(builder::apiKey);
        connectionInfo.endpoint().ifPresent(builder::baseUrl);
        return builder.build();
    }
}
