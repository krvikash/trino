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
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import io.trino.testing.QueryRunner;
import org.apache.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.util.List;

final class TestOpensearchSearchAfterConnectorTest
        extends BaseOpenSearchConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        opensearch = new OpenSearchServer(DEFAULT_LATEST_IMAGE, false, ImmutableMap.of());
        HostAndPort address = opensearch.getAddress();
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(address.getHost(), address.getPort())));

        return OpenSearchQueryRunner.builder(opensearch.getAddress())
                .addConnectorProperties(ImmutableMap.of("opensearch.search-strategy", "SEARCH_AFTER"))
                .setInitialTables(REQUIRED_TPCH_TABLES)
                .build();
    }

    @Override
    protected List<Integer> largeInValuesCountData()
    {
        // 1000 IN fails with "Query contains too many nested clauses; maxClauseCount is set to 1024"
        return ImmutableList.of(200, 500);
    }
}
