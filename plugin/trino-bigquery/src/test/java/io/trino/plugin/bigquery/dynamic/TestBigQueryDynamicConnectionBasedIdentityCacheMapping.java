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
package io.trino.plugin.bigquery.dynamic;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.base.cache.identity.IdentityCacheMapping.IdentityCacheKey;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestBigQueryDynamicConnectionBasedIdentityCacheMapping
{
    private static final String PROJECT_ID_CREDENTIAL_NAME = "project_id";
    private static final String PARENT_PROJECT_ID_CREDENTIAL_NAME = "parent_project_id";
    private static final String CREDENTIALS_KEY_CREDENTIAL_NAME = "credentials_key";
    private static final String VIEW_MATERIALIZATION_PROJECT_CREDENTIAL_NAME = "view_materialization_project";
    private static final String VIEW_MATERIALIZATION_DATASET_CREDENTIAL_NAME = "view_materialization_dataset";

    private final BigQueryDynamicConnectionBasedIdentityCacheMapping cacheMapping = new BigQueryDynamicConnectionBasedIdentityCacheMapping(
            new BigQueryDynamicConnectionPassthroughConfig()
                    .setProjectIdCredentialName(PROJECT_ID_CREDENTIAL_NAME)
                    .setParentProjectIdCredentialName(PARENT_PROJECT_ID_CREDENTIAL_NAME)
                    .setCredentialsKeyCredentialName(CREDENTIALS_KEY_CREDENTIAL_NAME)
                    .setViewMaterializationProjectCredentialName(VIEW_MATERIALIZATION_PROJECT_CREDENTIAL_NAME)
                    .setViewMaterializationDatasetCredentialName(VIEW_MATERIALIZATION_DATASET_CREDENTIAL_NAME));

    @Test
    void testCacheKeyUniqueness()
    {
        // Same credentials should produce equal cache keys
        assertThat(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-1", "view-dataset-1"))
                .isEqualTo(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-1", "view-dataset-1"));

        // Different project ID should produce different cache keys
        assertThat(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-1", "view-dataset-1"))
                .isNotEqualTo(createIdentityCacheKey("project-2", "parent-1", "key-1", "view-project-1", "view-dataset-1"));

        // Different parent project ID should produce different cache keys
        assertThat(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-1", "view-dataset-1"))
                .isNotEqualTo(createIdentityCacheKey("project-1", "parent-2", "key-1", "view-project-1", "view-dataset-1"));

        // Different credentials key should produce different cache keys
        assertThat(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-1", "view-dataset-1"))
                .isNotEqualTo(createIdentityCacheKey("project-1", "parent-1", "key-2", "view-project-1", "view-dataset-1"));

        // Different view materialization project should produce different cache keys
        assertThat(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-1", "view-dataset-1"))
                .isNotEqualTo(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-2", "view-dataset-1"));

        // Different view materialization dataset should produce different cache keys
        assertThat(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-1", "view-dataset-1"))
                .isNotEqualTo(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-1", "view-dataset-2"));

        // Different order of credential values should produce different cache keys
        assertThat(createIdentityCacheKey("project-1", "parent-1", "key-1", "view-project-1", "view-dataset-1"))
                .isNotEqualTo(createIdentityCacheKey("parent-1", "project-1", "key-1", "view-project-1", "view-dataset-1"));
    }

    private IdentityCacheKey createIdentityCacheKey(
            String projectId,
            String parentProjectId,
            String credentialsKey,
            String viewMaterializationProject,
            String viewMaterializationDataset)
    {
        return cacheMapping.getRemoteUserCacheKey(TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity
                        .forUser("test")
                        .withExtraCredentials(
                                ImmutableMap.<String, String>builder()
                                        .put(PROJECT_ID_CREDENTIAL_NAME, projectId)
                                        .put(PARENT_PROJECT_ID_CREDENTIAL_NAME, parentProjectId)
                                        .put(CREDENTIALS_KEY_CREDENTIAL_NAME, credentialsKey)
                                        .put(VIEW_MATERIALIZATION_PROJECT_CREDENTIAL_NAME, viewMaterializationProject)
                                        .put(VIEW_MATERIALIZATION_DATASET_CREDENTIAL_NAME, viewMaterializationDataset)
                                        .buildOrThrow())
                        .build())
                .build());
    }
}
