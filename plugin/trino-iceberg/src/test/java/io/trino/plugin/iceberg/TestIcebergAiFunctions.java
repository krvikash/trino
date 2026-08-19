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

import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.SystemSecurityMetadata;
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
import static com.starburstdata.trino.plugin.functions.ai.AiQueryRunner.starburstAiFileStorageProperties;
import static io.trino.spi.security.AccessDeniedException.denyExecuteAiModelAccess;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergAiFunctions
        extends AbstractTestQueryFramework
{
    private static final String DENY_ACCESS_MODEL = "deny_access_model";
    private static final String DENY_ACCESS_ROLE = "deny_access_role";
    private static final String ALLOW_ACCESS_ROLE = "allow_access_role";
    private static final String MODEL_PROVIDERS =
            """
            {
                "models": [
                    {
                      "id": "cohere",
                      "modelName": "cohere.embed-multilingual-v3",
                      "kind": "EMBED",
                      "connectionInfo": {
                        "provider": "AWS_BEDROCK",
                        "awsAccessKey": "${ENV:BEDROCK_ACCESS_KEY_ID}",
                        "awsSecretKey": "${ENV:BEDROCK_SECRET_ACCESS_KEY}",
                        "region": "us-east-1"
                      }
                    },
                    {
                        "id": "openai",
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
                        "modelName": "text-embedding-3-small",
                        "kind": "EMBED",
                        "connectionInfo": {
                            "provider": "OPENAI",
                            "endpoint": "https://api.openai.com/v1",
                            "apiKey": "${ENV:OPEN_AI_API_KEY}"
                        }
                    }
                ]
            }
            """.formatted(DENY_ACCESS_MODEL);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(starburstAiFileStorageProperties(MODEL_PROVIDERS))
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
                                    return Set.of(ALLOW_ACCESS_ROLE, DENY_ACCESS_ROLE);
                                }
                            });
                })
                .setAdditionalSetup(runner -> addStarburstAiCatalog(MODEL_PROVIDERS, runner))
                .amendSession(builder -> builder.setIdentity(Identity.forUser("user").withEnabledRoles(ImmutableSet.of(ALLOW_ACCESS_ROLE)).build()))
                .build();
    }

    @Test
    public void testGenerateEmbeddingsProcedure()
    {
        try (TestTable table = newTrinoTable("test_generate_embeddings_procedure", "(data VARCHAR)")) {
            assertUpdate("INSERT INTO %s VALUES 'apple', 'orange', 'cat', 'dog', null, '', 'shirt', 'pants'".formatted(table.getName()), 8);
            assertUpdate("ALTER TABLE %s ADD COLUMN embedding ARRAY(DOUBLE)".formatted(table.getName()));
            assertUpdate("ALTER TABLE %s EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'openai')".formatted(table.getName()));

            assertQuery(
                    "SELECT data FROM (SELECT data, cosine_similarity(embedding, starburst.ai.generate_embedding('animal', 'openai')) AS similarity FROM %s ORDER BY similarity DESC LIMIT 2)".formatted(table.getName()),
                    "VALUES 'cat', 'dog'");
        }

        try (TestTable table = newTrinoTable("test_generate_embeddings_procedure", "(data VARCHAR)")) {
            assertUpdate("INSERT INTO %s VALUES 'apple', 'orange', 'cat', 'dog', null, '', 'shirt', 'pants'".formatted(table.getName()), 8);
            assertUpdate("ALTER TABLE %s ADD COLUMN embedding ARRAY(REAL)".formatted(table.getName()));
            assertUpdate("ALTER TABLE %s EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'openai')".formatted(table.getName()));

            assertQuery(
                    "SELECT data FROM (SELECT data, cosine_similarity(embedding, starburst.ai.generate_embedding('animal', 'openai')) AS similarity FROM %s ORDER BY similarity DESC LIMIT 2)".formatted(table.getName()),
                    "VALUES 'cat', 'dog'");
        }

        try (TestTable table = newTrinoTable(
                "test_generate_embeddings_procedure_partitioned_table_",
                "(c1 INT, data VARCHAR) WITH (partitioning = ARRAY['c1', 'data'])")) {
            assertUpdate("INSERT INTO " + table.getName() + " VALUES (1, 'apple'), (2, 'orange'), (3, 'cat'), (4, 'dog'), (5, null), (6, ''), (7, 'shirt'), (8, 'pants')",
                    8);
            // Create embedding for partition column
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN embedding ARRAY(DOUBLE)");
            assertUpdate("ALTER TABLE " + table.getName() + " EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'openai')");
            assertQuery(
                    "SELECT data FROM (SELECT data, cosine_similarity(embedding, starburst.ai.generate_embedding('animal', 'openai')) AS similarity FROM "
                            + table.getName()
                            + " ORDER BY similarity DESC LIMIT 2)",
                    "VALUES 'cat', 'dog'");
            // Conditionally create embeddings
            assertUpdate("ALTER TABLE " + table.getName() + " ADD COLUMN data_orange_embedding ARRAY(DOUBLE)");
            assertUpdate("ALTER TABLE "
                    + table.getName()
                    + " EXECUTE generate_embeddings(embedding_column => 'data_orange_embedding', data_column => 'data', model_id => 'openai') WHERE c1 = 2");
            assertThat(query("SELECT c1 FROM " + table.getName() + " WHERE data_orange_embedding IS NOT NULL"))
                    .matches("VALUES 2");
        }
    }

    @Test
    public void testBinaryEmbeddings()
    {
        try (TestTable table = newTrinoTable("test_binary_embeddings_", "(data VARCHAR)")) {
            assertUpdate("INSERT INTO %s VALUES 'apple', 'orange', 'cat', 'dog', null, '', 'shirt', 'pants'".formatted(table.getName()), 8);
            assertUpdate("ALTER TABLE %s ADD COLUMN embedding VARBINARY".formatted(table.getName()));
            assertUpdate("ALTER TABLE %s EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'cohere')".formatted(table.getName()));

            assertQuery(
                    ("SELECT data FROM (SELECT data, hamming_distance(embedding, starburst.ai.generate_binary_embedding('clothing', 'cohere')) AS distance " +
                            "FROM %s ORDER BY distance ASC LIMIT 2)").formatted(table.getName()),
                    "VALUES 'shirt', 'pants'");
        }
    }

    @Test
    public void testAccessControl()
    {
        try (TestTable table = newTrinoTable("test_binary_embeddings_", "(data VARCHAR)")) {
            assertUpdate("INSERT INTO %s VALUES 'apple', 'orange', 'cat', 'dog', null, '', 'shirt', 'pants'".formatted(table.getName()), 8);
            assertUpdate("ALTER TABLE %s ADD COLUMN embedding VARBINARY".formatted(table.getName()));
            assertQuerySucceeds(
                    sessionWithRole(ALLOW_ACCESS_ROLE),
                    "ALTER TABLE %s EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'cohere')".formatted(table.getName()));
            assertQueryFails(
                    sessionWithRole(DENY_ACCESS_ROLE),
                    "ALTER TABLE %s EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => 'cohere')".formatted(table.getName()),
                    AccessDeniedException.PREFIX + "Cannot execute model cohere");
            assertQueryFails(
                    sessionWithRole(ALLOW_ACCESS_ROLE),
                    "ALTER TABLE %s EXECUTE generate_embeddings(embedding_column => 'embedding', data_column => 'data', model_id => '%s')".formatted(table.getName(), DENY_ACCESS_MODEL),
                    AccessDeniedException.PREFIX + "Cannot execute model %s".formatted(DENY_ACCESS_MODEL));
        }
    }

    private Session sessionWithRole(String role)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setIdentity(Identity.forUser("user").withEnabledRoles(Set.of(role)).build())
                .build();
    }

    public static class TestingAiModelAccessControl
            implements AiModelAccessControl
    {
        @Override
        public void checkCanExecuteModel(Context context, String modelId)
        {
            if (modelId.equals(DENY_ACCESS_MODEL) || context.connectorIdentity().getEnabledSystemRoles().contains(DENY_ACCESS_ROLE)) {
                denyExecuteAiModelAccess(modelId);
            }
        }
    }
}
