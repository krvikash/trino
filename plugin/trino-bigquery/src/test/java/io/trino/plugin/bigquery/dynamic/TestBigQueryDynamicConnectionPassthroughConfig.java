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
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

final class TestBigQueryDynamicConnectionPassthroughConfig
{
    @Test
    void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BigQueryDynamicConnectionPassthroughConfig.class)
                .setProjectIdCredentialName(null)
                .setParentProjectIdCredentialName(null)
                .setCredentialsKeyCredentialName(null)
                .setViewMaterializationProjectCredentialName(null)
                .setViewMaterializationDatasetCredentialName(null));
    }

    @Test
    void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("bigquery.project-id.credential-name", "project_id")
                .put("bigquery.parent-project-id.credential-name", "parent_project_id")
                .put("bigquery.credentials-key.credential-name", "credentials_key")
                .put("bigquery.view-materialization-project.credential-name", "view_materialization_project")
                .put("bigquery.view-materialization-dataset.credential-name", "view_materialization_dataset")
                .buildOrThrow();

        BigQueryDynamicConnectionPassthroughConfig expected = new BigQueryDynamicConnectionPassthroughConfig()
                .setProjectIdCredentialName("project_id")
                .setParentProjectIdCredentialName("parent_project_id")
                .setCredentialsKeyCredentialName("credentials_key")
                .setViewMaterializationProjectCredentialName("view_materialization_project")
                .setViewMaterializationDatasetCredentialName("view_materialization_dataset");

        assertFullMapping(properties, expected);
    }
}
