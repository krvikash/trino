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
package io.trino.plugin.warp.execution.stats;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.airlift.json.JsonMapperProvider;
import io.trino.plugin.warp.gen.stats.NativeStats;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class TestStatsWrapper
{
    private static JsonMapper jsonMapper;

    @BeforeAll
    public static void init()
    {
        jsonMapper = new JsonMapperProvider().get();
    }

    @Test
    public void testSerializedStatObject()
            throws JsonProcessingException
    {
        NativeStats dummyNotInNode = new NativeStats();
        dummyNotInNode.addread_time_wait_nanos(9); // not persist
        dummyNotInNode.addread_cache_md_chunk_hits(9); // not persist
        JsonNode jsonNode = jsonMapper.readerFor(List.class).readTree(jsonMapper.writeValueAsString(dummyNotInNode));
        String res = jsonMapper.writeValueAsString(dummyNotInNode);
        assertThat(jsonNode.get("read_time_wait_nanos")).isEqualTo(null);
        NativeStats deserializeObject = jsonMapper.readerFor(NativeStats.class).readValue(res);
        assertThat(deserializeObject.getread_time_wait_nanos()).isEqualTo(0);
        assertThat(deserializeObject.getread_cache_md_chunk_hits()).isEqualTo(0);
    }
}
