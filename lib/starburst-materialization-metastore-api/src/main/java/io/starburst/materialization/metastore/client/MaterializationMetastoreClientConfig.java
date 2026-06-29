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
package io.starburst.materialization.metastore.client;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import java.net.URI;

public class MaterializationMetastoreClientConfig
{
    private URI baseUri;
    private String metastoreId;

    public URI getBaseUri()
    {
        return baseUri;
    }

    @Config("materialization.metastore.base-uri")
    @ConfigDescription("Base URI of the materialization metastore REST service")
    public MaterializationMetastoreClientConfig setBaseUri(URI baseUri)
    {
        this.baseUri = baseUri;
        return this;
    }

    public String getMetastoreId()
    {
        return metastoreId;
    }

    @Config("materialization.metastore.id")
    @ConfigDescription("Identifier of the materialization metastore tenant this client targets")
    public MaterializationMetastoreClientConfig setMetastoreId(String metastoreId)
    {
        this.metastoreId = metastoreId;
        return this;
    }
}
