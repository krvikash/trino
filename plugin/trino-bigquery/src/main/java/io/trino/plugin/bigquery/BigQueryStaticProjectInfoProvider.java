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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.TableId;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;

public class BigQueryStaticProjectInfoProvider
        implements BigQueryProjectInfoProvider
{
    private final Optional<String> projectId;
    private final Optional<String> parentProjectId;
    private final Optional<String> viewMaterializationProject;
    private final Optional<String> viewMaterializationDataset;

    @Inject
    BigQueryStaticProjectInfoProvider(BigQueryConfig bigQueryConfig)
    {
        this.projectId = bigQueryConfig.getProjectId();
        this.parentProjectId = bigQueryConfig.getParentProjectId();
        this.viewMaterializationProject = bigQueryConfig.getViewMaterializationProject();
        this.viewMaterializationDataset = bigQueryConfig.getViewMaterializationDataset();
    }

    @Override
    public Optional<String> projectId(ConnectorSession session)
    {
        return projectId;
    }

    @Override
    public Optional<String> parentProjectId(ConnectorSession session)
    {
        return parentProjectId;
    }

    @Override
    public String viewMaterializationProject(ConnectorSession session, TableId remoteTableId)
    {
        return viewMaterializationProject.orElseGet(remoteTableId::getProject);
    }

    @Override
    public String viewMaterializationDataset(ConnectorSession session, TableId remoteTableId)
    {
        return viewMaterializationDataset.orElseGet(remoteTableId::getDataset);
    }
}
