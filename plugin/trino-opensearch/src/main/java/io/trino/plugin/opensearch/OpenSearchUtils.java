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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;

import java.io.IOException;

public final class OpenSearchUtils
{
    public static final Logger LOG = Logger.get(OpenSearchUtils.class);

    private OpenSearchUtils() {}

    public static boolean isScrollable(ObjectMapper objectMapper, String query)
    {
        try {
            JsonNode root = objectMapper.readTree(query);
            boolean hasAggs = root.has("aggs") || root.has("aggregations");
            boolean hasZeroSize = root.has("size") && root.get("size").asInt() == 0;
            return !hasAggs && !hasZeroSize;
        }
        catch (IOException e) {
            LOG.warn(e, "Unable to parse query: " + query);
            return false;
        }
    }
}
