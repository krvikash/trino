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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import org.junit.jupiter.api.Test;

import java.io.File;

import static com.google.common.base.Throwables.getCausalChain;
import static com.starburstdata.trino.plugin.ai.AiModule.getModelConnectionSpecs;
import static com.starburstdata.trino.plugin.ai.TestingUtils.createModelConnectionSpecsFile;
import static io.trino.plugin.base.util.JsonUtils.parseJson;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCreateModelConnectionSpecs
{
    @Test
    public void testShouldMaskSecretsWhenParsingFails()
    {
        String json = """
              {
                 "models": [
                     {
                         "id": "embed1",
                         "modelName": "cohere.embed-multilingual-v3",
                         "kind": "EMBED",
                         "dimensions": 768,
                         "connectionInfo": {
                             "provider": "AWS_BEDROCK",
                             "awsAccessKey": MYSECRET
                             "region": "us-east-1"
                         }
                     }
                 ]
              }""";

        File file = createModelConnectionSpecsFile(json);
        assertThatThrownBy(() -> getModelConnectionSpecs(new AiConfig().setModelConnectionSpecsFile(file.getAbsolutePath()), new SecretsResolver(ImmutableMap.of())))
                .satisfies(exception -> assertThat(getCausalChain(exception)
                        .stream()
                        .map(Throwable::getMessage))
                        .noneMatch(message -> message.contains("MYSECRET")));

        assertThatThrownBy(() -> parseJson(json, ModelConnectionSpecs.class))
                .satisfies(exception -> assertThat(getCausalChain(exception)
                        .stream()
                        .map(Throwable::getMessage))
                        .anyMatch(message -> message.contains("MYSECRET")));
    }
}
