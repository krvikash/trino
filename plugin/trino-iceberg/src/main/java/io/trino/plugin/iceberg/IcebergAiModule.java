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
package io.trino.plugin.iceberg;

import com.fasterxml.jackson.core.JsonParseException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.OptionalBinder;
import com.starburstdata.trino.plugin.ai.AiClientModule;
import com.starburstdata.trino.plugin.ai.ClientProvider;
import com.starburstdata.trino.plugin.ai.DisabledClientProvider;
import com.starburstdata.trino.plugin.ai.ModelClientProvider;
import com.starburstdata.trino.plugin.ai.ModelConnectionSpec;
import com.starburstdata.trino.plugin.ai.ModelConnectionSpecs;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.configuration.secrets.SecretsResolver;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.base.Throwables.getCausalChain;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugin.ai.AiErrorCode.AI_ERROR;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.base.util.JsonUtils.parseJson;

public class IcebergAiModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergAiConfig.class);
        OptionalBinder<ClientProvider> clientProviderBinder = newOptionalBinder(binder, ClientProvider.class);
        clientProviderBinder.setDefault().to(DisabledClientProvider.class).in(Scopes.SINGLETON);

        install(conditionalModule(
                IcebergAiConfig.class,
                config -> config.getModelConnectionSpecsFile() != null,
                conditionalBinder -> {
                    conditionalBinder.install(new AiClientModule());
                    clientProviderBinder.setBinding().to(ModelClientProvider.class).in(Scopes.SINGLETON);
                }));
    }

    @Provides
    public static List<ModelConnectionSpec> getModelConnectionSpecs(IcebergAiConfig config, SecretsResolver secretsResolver)
    {
        if (config.getModelConnectionSpecsFile() == null) {
            return List.of();
        }

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
