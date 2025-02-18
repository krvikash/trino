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
import io.airlift.log.Logger;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.io.File;

import static com.starburstdata.trino.plugin.ai.TestingUtils.createModelConnectionSpecsFile;
import static io.trino.testing.TestingSession.testSessionBuilder;

public final class AiQueryRunner
{
    private AiQueryRunner() {}

    public static void main(String[] args)
            throws Exception
    {
        String json = """
                {
                    "models": [
                        {
                            "id": "titan_embed_v2",
                            "modelName": "amazon.titan-embed-text-v2:0",
                            "kind": "EMBED",
                            "connectionInfo": {
                                "provider": "AWS_BEDROCK",
                                "region": "us-east-1"
                            }
                        },
                        {
                            "id": "meta_llama",
                            "modelName": "us.meta.llama3-3-70b-instruct-v1:0",
                            "kind": "GENERATE",
                            "maxTokens": 1024,
                            "temperature": 0.7,
                            "prompts": {
                                "systemPrompts": [
                                    "You are a useful assistant"
                                ],
                                "analyzeSentimentPrompt": "Analyze the sentiment of the text below. Classify it using one of the following categories: [positive, negative, neutral, mixed]. Use the following labels for the categories: positive = rad, negative = bummer, neutral = meh, neutral = so-so. Output only the label. Do not output anything else.\\n=====%s",
                                "maskPrompt": "Mask the values for each of the JSON encoded labels in the text below. Labels: %s\\nReplace the values with the text \\"[***]\\". Output only the masked text. Do not output anything else.\\n=====%s"
                            },
                            "connectionInfo": {
                                "provider": "AWS_BEDROCK",
                                "region": "us-east-1"
                            }
                        },
                        {
                            "id": "embedding3_small",
                            "modelName": "text-embedding-3-small",
                            "kind": "EMBED",
                            "connectionInfo": {
                                "provider": "OPENAI",
                                "endpoint": "https://api.openai.com/v1",
                                "apiKey": "${ENV:OPEN_AI_API_KEY}"
                            }
                        },
                        {
                            "id": "gpt4o_mini",
                            "modelName": "gpt-4o-mini",
                            "kind": "GENERATE",
                            "temperature": 0.7,
                            "connectionInfo": {
                                "provider": "OPENAI",
                                "endpoint": "https://api.openai.com/v1",
                                "apiKey": "${ENV:OPEN_AI_API_KEY}"
                            }
                        }
                    ]
                }
                """.stripIndent();

        File file = createModelConnectionSpecsFile(json);
        QueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build())
                .addCoordinatorProperty("http-server.http.port", "8080")
                .addCoordinatorProperty("sql.path", "ai")
                .build();
        queryRunner.installPlugin(new AiPlugin());
        queryRunner.createCatalog("ai", "starburst_ai", ImmutableMap.<String, String>builder()
                .put("ai.models-file", file.getAbsolutePath())
                .buildOrThrow());
        Logger log = Logger.get(AiQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
