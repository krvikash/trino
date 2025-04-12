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
package io.starburst.ai.client;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.starburst.ai.client.AiClientConfig.StorageType;
import io.starburst.ai.client.bedrock.AwsBedrockClientFactory;
import io.starburst.ai.client.bedrock.AwsBedrockEmbeddingCodecsModule;
import io.starburst.ai.client.openai.OpenAiClientFactory;

import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class AiClientModule
        extends AbstractConfigurationAwareModule
{
    @Override
    public void setup(Binder binder)
    {
        configBinder(binder).bindConfig(AiClientConfig.class);
        binder.install(new AwsBedrockEmbeddingCodecsModule());
        binder.bind(AwsBedrockClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(OpenAiClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(PromptDao.class).to(StaticPromptDao.class).in(Scopes.SINGLETON);
        binder.bind(ReloadingModelClientProvider.class).in(Scopes.SINGLETON);
        newExporter(binder).export(ReloadingModelClientProvider.class).withGeneratedName();
        binder.bind(ModelClientProviderWithDao.class).to(ReloadingModelClientProvider.class).in(Scopes.SINGLETON);
        install(conditionalModule(AiClientConfig.class,
                        aiClientConfig -> aiClientConfig.getStorageType() == StorageType.NONE,
                        innerBinder -> innerBinder.bind(ModelConnectionSpecsLoader.class).toInstance(ModelConnectionSpecsLoader.EMPTY_LOADER)));
        install(conditionalModule(AiClientConfig.class,
                aiClientConfig -> aiClientConfig.getStorageType() == StorageType.FILE,
                new FileBackedModelSpecModule()));
    }

    @Provides
    @Singleton
    public ModelClientProvider getModelClientProvider(ModelClientProviderWithDao modelClientProviderWithDao)
    {
        return modelClientProviderWithDao;
    }

    @Provides
    @Singleton
    public ModelConnectionSpecDao getModelConnectionSpecDao(ModelClientProviderWithDao modelClientProviderWithDao)
    {
        return modelClientProviderWithDao;
    }
}
