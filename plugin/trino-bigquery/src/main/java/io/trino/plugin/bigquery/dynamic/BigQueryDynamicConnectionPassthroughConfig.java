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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

public class BigQueryDynamicConnectionPassthroughConfig
{
    private String projectIdCredentialName;
    private String parentProjectIdCredentialName;
    private String credentialsKeyCredentialName;
    private String viewMaterializationProjectCredentialName;
    private String viewMaterializationDatasetCredentialName;

    @NotNull
    public String getProjectIdCredentialName()
    {
        return projectIdCredentialName;
    }

    @Config("bigquery.project-id.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the BigQuery project id to use for the connection")
    public BigQueryDynamicConnectionPassthroughConfig setProjectIdCredentialName(String projectIdCredentialName)
    {
        this.projectIdCredentialName = projectIdCredentialName;
        return this;
    }

    @NotNull
    public String getParentProjectIdCredentialName()
    {
        return parentProjectIdCredentialName;
    }

    @Config("bigquery.parent-project-id.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the BigQuery parent project id to use for the connection, if applicable")
    public BigQueryDynamicConnectionPassthroughConfig setParentProjectIdCredentialName(String parentProjectIdCredentialName)
    {
        this.parentProjectIdCredentialName = parentProjectIdCredentialName;
        return this;
    }

    @NotNull
    public String getCredentialsKeyCredentialName()
    {
        return credentialsKeyCredentialName;
    }

    @Config("bigquery.credentials-key.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the base64 encoded credentials key to use for the connection")
    public BigQueryDynamicConnectionPassthroughConfig setCredentialsKeyCredentialName(String credentialsKeyCredentialName)
    {
        this.credentialsKeyCredentialName = credentialsKeyCredentialName;
        return this;
    }

    @NotNull
    public String getViewMaterializationProjectCredentialName()
    {
        return viewMaterializationProjectCredentialName;
    }

    @Config("bigquery.view-materialization-project.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the BigQuery project where the materialized view is going to be created")
    public BigQueryDynamicConnectionPassthroughConfig setViewMaterializationProjectCredentialName(String viewMaterializationProjectCredentialName)
    {
        this.viewMaterializationProjectCredentialName = viewMaterializationProjectCredentialName;
        return this;
    }

    @NotNull
    public String getViewMaterializationDatasetCredentialName()
    {
        return viewMaterializationDatasetCredentialName;
    }

    @Config("bigquery.view-materialization-dataset.credential-name")
    @ConfigDescription("Name of the extra credential key that contains the BigQuery dataset where the materialized view is going to be created")
    public BigQueryDynamicConnectionPassthroughConfig setViewMaterializationDatasetCredentialName(String viewMaterializationDatasetCredentialName)
    {
        this.viewMaterializationDatasetCredentialName = viewMaterializationDatasetCredentialName;
        return this;
    }
}
