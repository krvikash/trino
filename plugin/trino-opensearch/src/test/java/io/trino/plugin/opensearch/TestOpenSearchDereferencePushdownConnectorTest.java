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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.assertions.Assert.assertEventually;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.opensearch.action.search.SearchType.QUERY_THEN_FETCH;

@TestInstance(PER_CLASS)
public class TestOpenSearchDereferencePushdownConnectorTest
        extends TestOpenSearchLatestConnectorTest
{
    @Test
    public void testDereferencePushdown()
            throws IOException
    {
        String index = "test_dereference_pushdown" + randomNameSuffix();
        IndexRequest request = new IndexRequest(index)
                .id("1")
                .create(true)
                .source(
                        Map.of(
                                "array_string_field", List.of("trino", "the", "lean", "machine-ohs"),
                                "object_field_outer", Map.of(
                                        "array_string_field", List.of("trino", "the", "lean", "machine-ohs"),
                                        "string_field_outer", "sample",
                                        "int_field_outer", 44),
                                "long_field", 314159265359L,
                                "id_field", "564e6982-88ee-4498-aa98-df9e3f6b6109",
                                "timestamp_field", "1987-09-17T06:22:48.000Z",
                                "object_field", Map.of(
                                        "array_string_field", List.of("trino", "the", "lean", "machine-ohs"),
                                        "string_field", "sample",
                                        "int_field", 2,
                                        "object_field_2", Map.of(
                                                "array_string_field", List.of("trino", "the", "lean", "machine-ohs"),
                                                "string_field2", "sample",
                                                "int_field", 33,
                                                "object_field_3", Map.of(
                                                        "array_string_field", List.of("trino", "the", "lean", "machine-ohs"),
                                                        "string_field3", "sample",
                                                        "int_field3", 55.45)))));
        client.index(request, RequestOptions.DEFAULT)
                .forcedRefresh();

        SearchResponse response = client.search(new SearchRequest(index), RequestOptions.DEFAULT);

        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .filter(QueryBuilders.termQuery("object_field_outer.int_field_outer", 44));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder)
                .fetchField("id_field");
//                .docValueField("id_field")
//                .fetchSource("id_field", null)

        SearchRequest searchRequest = new SearchRequest(index)
                .searchType(QUERY_THEN_FETCH)
                .source(searchSourceBuilder);
//                .preference("_shards:" + shard)
//                .scroll(new TimeValue(scrollTimeout.toMillis()))

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        assertEventually(() -> assertQuery("select object_field_outer.int_field_outer from " + index + " where  object_field_outer.int_field_outer=44", "VALUES 44"));
        assertQuery("select id_field, object_field.object_field_2.object_field_3.string_field3 from " + index + " where  object_field_outer.int_field_outer=44", "VALUES ('564e6982-88ee-4498-aa98-df9e3f6b6109', 'sample')");
        assertQuery("select object_field_outer.int_field_outer from " + index + " where  object_field_outer.int_field_outer=44", "VALUES 44");
//        assertThat(query("SELECT row_field.first.second FROM TABLE(mongodb.system.query(database => 'tpch', collection => '" + tableName + "', filter => '{ \"row_field.first.second\": 1 }'))"))
//                .matches("VALUES BIGINT '1'")
//                .isFullyPushedDown();
    }
}
