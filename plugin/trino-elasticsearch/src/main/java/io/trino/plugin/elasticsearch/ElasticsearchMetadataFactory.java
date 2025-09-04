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
package io.trino.plugin.elasticsearch;

import com.google.inject.Inject;
import io.trino.plugin.elasticsearch.client.ElasticsearchClientFactory;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.TypeManager;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class ElasticsearchMetadataFactory
{
    private final Function<ConnectorSession, ElasticsearchMetadata> metadataSupplier;

    @Inject
    public ElasticsearchMetadataFactory(TypeManager typeManager, ElasticsearchConfig config, ElasticsearchClientFactory clientFactory)
    {
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(config, "config is null");
        requireNonNull(clientFactory, "clientFactory is null");
        this.metadataSupplier = session -> new ElasticsearchMetadata(typeManager, clientFactory.createClient(session), config);
    }

    public ElasticsearchMetadata create(ConnectorSession session)
    {
        return metadataSupplier.apply(session);
    }
}
