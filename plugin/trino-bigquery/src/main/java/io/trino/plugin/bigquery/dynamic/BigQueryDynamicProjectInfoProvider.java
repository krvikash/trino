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

import com.google.cloud.bigquery.TableId;
import com.google.inject.Inject;
import io.trino.plugin.bigquery.BigQueryProjectInfoProvider;
import io.trino.spi.connector.ConnectorSession;

import java.util.Map;
import java.util.Optional;

public class BigQueryDynamicProjectInfoProvider
        implements BigQueryProjectInfoProvider
{
    private final String projectIdCredentialName;
    private final String parentProjectIdCredentialName;
    private final String viewMaterializationProjectCredentialName;
    private final String viewMaterializationDatasetCredentialName;

    @Inject
    public BigQueryDynamicProjectInfoProvider(BigQueryDynamicConnectionPassthroughConfig dynamicConnectionPassthroughConfig)
    {
        this.projectIdCredentialName = dynamicConnectionPassthroughConfig.getProjectIdCredentialName();
        this.parentProjectIdCredentialName = dynamicConnectionPassthroughConfig.getParentProjectIdCredentialName();
        this.viewMaterializationProjectCredentialName = dynamicConnectionPassthroughConfig.getViewMaterializationProjectCredentialName();
        this.viewMaterializationDatasetCredentialName = dynamicConnectionPassthroughConfig.getViewMaterializationDatasetCredentialName();
    }

    @Override
    public Optional<String> projectId(ConnectorSession session)
    {
        return Optional.ofNullable(session.getIdentity().getExtraCredentials().get(projectIdCredentialName));
    }

    @Override
    public Optional<String> parentProjectId(ConnectorSession session)
    {
        return Optional.ofNullable(session.getIdentity().getExtraCredentials().get(parentProjectIdCredentialName));
    }

    @Override
    public String viewMaterializationProject(ConnectorSession connectorSession, TableId remoteTableId)
    {
        Map<String, String> extraCredentials = connectorSession.getIdentity().getExtraCredentials();
        return extraCredentials.getOrDefault(viewMaterializationProjectCredentialName, remoteTableId.getProject());
    }

    @Override
    public String viewMaterializationDataset(ConnectorSession connectorSession, TableId remoteTableId)
    {
        Map<String, String> extraCredentials = connectorSession.getIdentity().getExtraCredentials();
        return extraCredentials.getOrDefault(viewMaterializationDatasetCredentialName, remoteTableId.getDataset());
    }
}
