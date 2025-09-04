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
package io.trino.plugin.kafka.schema.confluent;

import com.fasterxml.jackson.databind.JsonNode;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.json.JsonFieldDecoder;
import io.trino.plugin.kafka.utils.JsonBlockUtils;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public class JsonArrayFieldDecoder
        implements JsonFieldDecoder
{
    private final ArrayType type;

    public JsonArrayFieldDecoder(ArrayType type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public FieldValueProvider decode(JsonNode value)
    {
        return new JsonArrayFieldValueProvider(value, type);
    }

    private static class JsonArrayFieldValueProvider
            extends FieldValueProvider
    {
        private final JsonNode value;
        private final Type elementType;

        public JsonArrayFieldValueProvider(JsonNode value, ArrayType type)
        {
            this.value = requireNonNull(value, "value is null");
            this.elementType = requireNonNull(type, "type is null").getElementType();
        }

        @Override
        public boolean isNull()
        {
            return value.isMissingNode() || value.isNull();
        }

        @Override
        public Object getObject()
        {
            BlockBuilder builder = elementType.createBlockBuilder(null, value.size());
            for (JsonNode element : value) {
                JsonBlockUtils.write(builder, element, elementType);
            }
            return builder.build();
        }
    }
}
