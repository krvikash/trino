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

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.starburstdata.trino.plugin.ai.bedrock.AwsBedrockClientFactory;
import com.starburstdata.trino.plugin.ai.bedrock.AwsBedrockEmbeddingCodecsModule;
import com.starburstdata.trino.plugin.ai.openai.OpenAiClientFactory;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.FunctionProvider;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.base.Throwables.getCausalChain;
import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static com.starburstdata.trino.plugin.ai.AiErrorCode.AI_ERROR;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.util.JsonUtils.parseJson;

public class AiModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AiConfig.class);
        binder.bind(AiConnector.class).in(Scopes.SINGLETON);
        binder.bind(AiMetadata.class).in(Scopes.SINGLETON);
        binder.bind(AiFunctions.class).in(Scopes.SINGLETON);

        binder.bind(Connector.class).to(AiConnector.class).in(Scopes.SINGLETON);
        binder.bind(ConnectorMetadata.class).to(AiMetadata.class).in(Scopes.SINGLETON);
        binder.bind(FunctionProvider.class).to(AiFunctions.class).in(Scopes.SINGLETON);

        binder.bind(PromptProvider.class).to(StaticPromptProvider.class).in(Scopes.SINGLETON);
        binder.bind(ModelConnectionSpecDao.class).to(StaticModelConnectionSpecDao.class).in(Scopes.SINGLETON);

        binder.install(new AwsBedrockEmbeddingCodecsModule());
        binder.bind(AwsBedrockClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(OpenAiClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(ClientFactory.class).in(Scopes.SINGLETON);

        var systemTableBinder = newSetBinder(binder, SystemTable.class);
        systemTableBinder.addBinding().to(LanguageModelSystemTable.class).in(Scopes.SINGLETON);
        systemTableBinder.addBinding().to(EmbeddingModelSystemTable.class).in(Scopes.SINGLETON);
    }

    @Provides
    public static List<FunctionMetadata> getFunctionMetadata(AiFunctions functions)
    {
        return functions.getFunctions();
    }

    @Provides
    public static List<ModelConnectionSpec> getModelConnectionSpecs(AiConfig config, SecretsResolver secretsResolver)
    {
        try {
            String json = Files.readString(Path.of(config.getModelConnectionSpecsFile()));
            String resolvedJson = secretsResolver.getResolvedConfiguration(ImmutableMap.of("json", json)).get("json");
            return parseJson(resolvedJson, ModelConnectionSpecs.class).models();
        }
        catch (RuntimeException e) {
            // the error message can contain sensitive information, so just include the location of the parsing error
            getCausalChain(e).stream()
                    .filter(JsonParseException.class::isInstance)
                    .map(JsonParseException.class::cast)
                    .findFirst()
                    .ifPresent(jpe -> {
                        throw new TrinoException(AI_ERROR, "Error parsing AI model connection spec at: %s".formatted(jpe.getLocation().offsetDescription()));
                    });
            throw e;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
