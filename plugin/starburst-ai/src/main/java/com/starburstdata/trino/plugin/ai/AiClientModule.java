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

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.starburstdata.trino.plugin.ai.bedrock.AwsBedrockClientFactory;
import com.starburstdata.trino.plugin.ai.bedrock.AwsBedrockEmbeddingCodecsModule;
import com.starburstdata.trino.plugin.ai.openai.OpenAiClientFactory;

public class AiClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(PromptProvider.class).to(StaticPromptProvider.class).in(Scopes.SINGLETON);
        binder.bind(ModelConnectionSpecDao.class).to(StaticModelConnectionSpecDao.class).in(Scopes.SINGLETON);
        binder.install(new AwsBedrockEmbeddingCodecsModule());
        binder.bind(AwsBedrockClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(OpenAiClientFactory.class).in(Scopes.SINGLETON);
    }
}
