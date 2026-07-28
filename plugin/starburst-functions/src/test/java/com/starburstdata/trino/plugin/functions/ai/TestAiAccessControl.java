/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
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
package com.starburstdata.trino.plugin.functions.ai;

import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.SystemSecurityMetadata;
import io.trino.plugin.memory.MemoryQueryRunner;
import io.trino.spi.security.AccessDeniedException;
import io.trino.spi.security.AiModelAccessControl;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.addStarburstAiCatalog;
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.sessionWithRole;
import static io.trino.spi.security.AccessDeniedException.denyExecuteAiModelAccess;

public class TestAiAccessControl
        extends AbstractTestQueryFramework
{
    private static final String ALLOW_ROLE = "allow-role";
    private static final String DENY_ROLE = "deny-role";
    private static final String EMBED_ALLOW_MODEL = "embed-allow";
    private static final String EMBED_DENY_MODEL = "embed-deny";
    private static final String LANGUAGE_ALLOW_MODEL = "language-allow";
    private static final String LANGUAGE_DENY_MODEL = "language-deny";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        String modelSpecs =
                """
                   {
                    "models": [
                        {
                          "id": "%s",
                          "modelName": "amazon.titan-embed-text-v2:0",
                          "kind": "EMBED",
                          "connectionInfo": {
                            "provider": "AWS_BEDROCK",
                            "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                            "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                            "region": "us-east-2"
                          }
                        },
                        {
                          "id": "%s",
                          "modelName": "text-embedding-3-small",
                          "kind": "EMBED",
                          "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1",
                            "apiKey": "${ENV:OPEN_AI_API_KEY}"
                          }
                        },
                        {
                            "id": "%s",
                            "modelName": "gpt-4o-mini",
                            "kind": "GENERATE",
                            "connectionInfo": {
                                "provider": "OPENAI",
                                "endpoint": "https://api.openai.com/v1",
                                "apiKey": "${ENV:OPEN_AI_API_KEY}"
                            }
                        },
                        {
                            "id": "%s",
                            "modelName": "gpt-4o-mini",
                            "kind": "GENERATE",
                            "connectionInfo": {
                                "provider": "OPENAI",
                                "endpoint": "https://api.openai.com/v1",
                                "apiKey": "${ENV:OPEN_AI_API_KEY}"
                            }
                        }
                    ]
                }
                """.formatted(EMBED_ALLOW_MODEL, EMBED_DENY_MODEL, LANGUAGE_ALLOW_MODEL, LANGUAGE_DENY_MODEL);

        return MemoryQueryRunner.builder()
                .setAdditionalModule(binder -> {
                    newOptionalBinder(binder, AiModelAccessControl.class)
                            .setBinding()
                            .to(TestingAiModelAccessControl.class)
                            .in(SINGLETON);
                    newOptionalBinder(binder, SystemSecurityMetadata.class)
                            .setBinding()
                            .toInstance(new DisabledSystemSecurityMetadata()
                            {
                                @Override
                                public Set<String> listEnabledRoles(Identity identity)
                                {
                                    return Set.of(ALLOW_ROLE, DENY_ROLE);
                                }
                            });
                })
                .setAdditionalSetup(runner -> addStarburstAiCatalog(modelSpecs, runner))
                .build();
    }

    @Test
    public void testPrompt()
    {
        String prompt = "Who let the dogs out?";
        assertQuerySucceeds(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.prompt('%s', '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL));
        assertQueryFails(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.prompt('%s', '%s')".formatted(prompt, LANGUAGE_DENY_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_DENY_MODEL);
        assertQueryFails(
                sessionWithRole(DENY_ROLE),
                "SELECT ai.prompt('%s', '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_ALLOW_MODEL);
    }

    @Test
    public void testPromptSystem()
    {
        String prompt = "Who let the dogs out?";
        assertQuerySucceeds(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.prompt('woof', '%s', '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL));
        assertQueryFails(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.prompt('woof', '%s', '%s')".formatted(prompt, LANGUAGE_DENY_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_DENY_MODEL);
        assertQueryFails(
                sessionWithRole(DENY_ROLE),
                "SELECT ai.prompt('woof', '%s', '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_ALLOW_MODEL);
    }

    @Test
    public void testClassify()
    {
        String prompt = "Who let the dogs out?";
        assertQuerySucceeds(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.classify('%s', ARRAY['positive', 'negative', 'neutral'], '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL));
        assertQueryFails(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.classify('%s', ARRAY['positive', 'negative', 'neutral'], '%s')".formatted(prompt, LANGUAGE_DENY_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_DENY_MODEL);
        assertQueryFails(
                sessionWithRole(DENY_ROLE),
                "SELECT ai.classify('%s', ARRAY['positive', 'negative', 'neutral'], '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_ALLOW_MODEL);
    }

    @Test
    public void testMask()
    {
        String prompt = "Who let the dogs out?";
        assertQuerySucceeds(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.mask('%s', ARRAY['credit card number', 'password'], '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL));
        assertQueryFails(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.mask('%s', ARRAY['credit card number', 'password'], '%s')".formatted(prompt, LANGUAGE_DENY_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_DENY_MODEL);
        assertQueryFails(
                sessionWithRole(DENY_ROLE),
                "SELECT ai.mask('%s', ARRAY['credit card number', 'password'], '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_ALLOW_MODEL);
    }

    @Test
    public void testTranslate()
    {
        String prompt = "Who let the dogs out?";
        assertQuerySucceeds(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.translate('%s', 'Spanish', '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL));
        assertQueryFails(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.translate('%s', 'Spanish', '%s')".formatted(prompt, LANGUAGE_DENY_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_DENY_MODEL);
        assertQueryFails(
                sessionWithRole(DENY_ROLE),
                "SELECT ai.translate('%s', 'Spanish', '%s')".formatted(prompt, LANGUAGE_ALLOW_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + LANGUAGE_ALLOW_MODEL);
    }

    @Test
    public void testGenerateEmbeddings()
    {
        String prompt = "Who let the dogs out?";
        assertQuerySucceeds(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.generate_embedding('%s', '%s')".formatted(prompt, EMBED_ALLOW_MODEL));
        assertQueryFails(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.generate_embedding('%s', '%s')".formatted(prompt, EMBED_DENY_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + EMBED_DENY_MODEL);
        assertQueryFails(
                sessionWithRole(DENY_ROLE),
                "SELECT ai.generate_embedding('%s', '%s')".formatted(prompt, EMBED_ALLOW_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + EMBED_ALLOW_MODEL);
    }

    @Test
    public void testGenerateBinaryEmbeddings()
    {
        String prompt = "Who let the dogs out?";
        assertQuerySucceeds(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.generate_binary_embedding('%s', '%s')".formatted(prompt, EMBED_ALLOW_MODEL));
        assertQueryFails(
                sessionWithRole(ALLOW_ROLE),
                "SELECT ai.generate_binary_embedding('%s', '%s')".formatted(prompt, EMBED_DENY_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + EMBED_DENY_MODEL);
        assertQueryFails(
                sessionWithRole(DENY_ROLE),
                "SELECT ai.generate_binary_embedding('%s', '%s')".formatted(prompt, EMBED_ALLOW_MODEL),
                AccessDeniedException.PREFIX + "Cannot execute model " + EMBED_ALLOW_MODEL);
    }

    @Test
    public void testGenerateEmbeddingsTableFunction()
    {
        try (TestTable table = newTrinoTable(
                "memory.default.test_generate_embeddings_table_function_",
                "(data VARCHAR, embedding ARRAY(DOUBLE))")) {
            assertQuerySucceeds(sessionWithRole(ALLOW_ROLE), updateWithModel(table.getName(), EMBED_ALLOW_MODEL));
            assertQueryFails(
                    sessionWithRole(ALLOW_ROLE),
                    updateWithModel(table.getName(), EMBED_DENY_MODEL),
                    AccessDeniedException.PREFIX + "Cannot execute model " + EMBED_DENY_MODEL);
            assertQueryFails(
                    sessionWithRole(DENY_ROLE),
                    updateWithModel(table.getName(), EMBED_ALLOW_MODEL),
                    AccessDeniedException.PREFIX + "Cannot execute model " + EMBED_ALLOW_MODEL);
        }
    }

    private static String updateWithModel(String tableName, String modelId)
    {
        return """
               INSERT INTO %s (data, embedding)
               SELECT data, embedding
               FROM TABLE(starburst.ai.generate_embeddings(
                 embedding_column => DESCRIPTOR(embedding),
                 data_column => DESCRIPTOR(data),
                 source => TABLE(SELECT * FROM (VALUES 'apple', 'orange', null, '', 'cat', 'dog', 'shirt', 'pants') AS t (data)),
                 model_id => '%s'))
               """.formatted(tableName, modelId);
    }

    public static class TestingAiModelAccessControl
            implements AiModelAccessControl
    {
        @Override
        public void checkCanExecuteModel(Context context, String modelId)
        {
            if (modelId.equals(LANGUAGE_DENY_MODEL) || modelId.equals(EMBED_DENY_MODEL) ||
                    context.connectorIdentity().getEnabledSystemRoles().contains(DENY_ROLE)) {
                denyExecuteAiModelAccess(modelId);
            }
        }
    }
}
