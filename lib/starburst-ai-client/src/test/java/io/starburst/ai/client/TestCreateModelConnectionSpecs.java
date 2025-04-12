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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.secrets.SecretsResolver;
import org.junit.jupiter.api.Test;

import java.io.File;

import static com.google.common.base.Throwables.getCausalChain;
import static io.starburst.ai.client.TestingUtils.createModelConnectionSpecsFile;
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

        FileBackedModelConnectionSpecsLoader reader = new FileBackedModelConnectionSpecsLoader(
                new AiFileStorageConfig().setModelConnectionSpecsFile(file.getPath()),
                new SecretsResolver(ImmutableMap.of()));
        assertThatThrownBy(reader::load)
                .satisfies(exception -> assertThat(getCausalChain(exception)
                        .stream()
                        .map(Throwable::getMessage))
                        .noneMatch(message -> message.contains("MYSECRET")));
    }
}
