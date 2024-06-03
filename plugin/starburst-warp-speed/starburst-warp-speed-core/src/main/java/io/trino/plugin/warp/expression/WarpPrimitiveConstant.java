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
package io.trino.plugin.warp.expression;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;

@JsonTypeName("primitive")
public class WarpPrimitiveConstant
        extends WarpConstant
{
    public static final WarpConstant TRUE = new WarpPrimitiveConstant(true, BooleanType.BOOLEAN);
    public static final WarpConstant FALSE = new WarpPrimitiveConstant(false, BooleanType.BOOLEAN);

    private final Object value;

    @JsonCreator
    public WarpPrimitiveConstant(@JsonProperty("value") Object value, @JsonProperty("type") Type type)
    {
        super(type);
        this.value = value;
    }

    @Override
    public Object getValue()
    {
        return value;
    }

    @Override
    public String getValueAsString()
    {
        return (value != null) ? value.toString() : "";
    }
}
