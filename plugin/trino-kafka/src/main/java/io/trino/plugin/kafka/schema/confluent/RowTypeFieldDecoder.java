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
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.trino.decoder.FieldValueProvider;
import io.trino.decoder.json.JsonFieldDecoder;
import io.trino.plugin.kafka.utils.JsonBlockUtils;
import io.trino.spi.type.RowType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static java.util.Objects.requireNonNull;

public class RowTypeFieldDecoder
        implements JsonFieldDecoder
{
    private final RowType rowType;

    public RowTypeFieldDecoder(RowType rowType)
    {
        this.rowType = requireNonNull(rowType, "rowType is null");
    }

    @Override
    public FieldValueProvider decode(JsonNode value)
    {
        return new RowTypeFieldValueProvider(value, rowType);
    }

    private static class RowTypeFieldValueProvider
            extends FieldValueProvider
    {
        private final JsonNode value;
        private final RowType rowType;

        public RowTypeFieldValueProvider(JsonNode value, RowType rowType)
        {
            this.value = requireNonNull(value, "value is null");
            this.rowType = requireNonNull(rowType, "rowType is null");
        }

        @Override
        public boolean isNull()
        {
            return value.isMissingNode() || value.isNull();
        }

        @Override
        public Object getObject()
        {
            return buildRowValue(rowType, fieldBuilders -> {
                List<RowType.Field> fields = rowType.getFields();
                checkArgument(value instanceof ObjectNode, "Expected ObjectNode, but got %s", value.getClass().getSimpleName());
                ObjectNode objectNode = (ObjectNode) value;
                for (int i = 0; i < fields.size(); i++) {
                    RowType.Field field = fields.get(i);
                    JsonBlockUtils.write(fieldBuilders.get(i), objectNode.get(field.getName().orElseThrow()), field.getType());
                }
            });
        }
    }
}
