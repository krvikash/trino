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
package io.trino.plugin.warp.dispatcher;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonMapperProvider;
import io.airlift.slice.Slice;
import io.trino.block.BlockJsonSerde;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.plugin.base.util.ConnectorExpressionUtil.ExpressionAndAssignments;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.rewrite.WarpExpression;
import io.trino.plugin.warp.util.json.SliceSerializer;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.metrics.Metric;
import io.trino.spi.metrics.Metrics;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class DispatcherTableHandleJsonTest
{
    @Test
    public void testJsonRoundTrip()
            throws Exception
    {
        ObjectMapper objectMapper = createObjectMapper();

        DispatcherTableHandle handle = new DispatcherTableHandle(
                "schemaName",
                "tableName",
                OptionalLong.of(7),
                TupleDomain.all(),
                new SimplifiedColumns(Set.of(new RegularColumn("col1"))),
                new ProxyTableHandle("proxyName"),
                Optional.of(new WarpExpression(new WarpCall("func", emptyList(), INTEGER), emptyList())),
                new Metrics(ImmutableMap.<String, Metric<?>>of("stat", new LongCount(3))),
                true,
                Optional.of(ExpressionAndAssignments.TRUE));

        String json = objectMapper.writeValueAsString(handle);
        DispatcherTableHandle roundTripped = objectMapper.readValue(json, DispatcherTableHandle.class);

        assertThat(roundTripped).isEqualTo(handle);
        // equals excludes metrics and originalExpression; assert them explicitly
        assertThat(roundTripped.getMetrics()).isEqualTo(handle.getMetrics());
        assertThat(roundTripped.getOriginalExpression()).isEqualTo(handle.getOriginalExpression());
    }

    private static ObjectMapper createObjectMapper()
    {
        JsonMapperProvider provider = new JsonMapperProvider();
        provider.setJsonSerializers(ImmutableMap.of(
                Slice.class, new SliceSerializer(),
                Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)));
        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                Block.class, new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE)));
        ObjectMapper objectMapper = provider.get();
        SimpleModule module = new SimpleModule();
        module.addAbstractTypeMapping(ConnectorTableHandle.class, ProxyTableHandle.class);
        objectMapper.registerModule(module);
        return objectMapper;
    }

    public record ProxyTableHandle(@JsonProperty("name") String name)
            implements ConnectorTableHandle {}
}
