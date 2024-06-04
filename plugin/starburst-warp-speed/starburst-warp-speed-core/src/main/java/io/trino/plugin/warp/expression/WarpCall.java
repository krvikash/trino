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
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

@JsonTypeName("call")
public class WarpCall
        implements WarpExpression
{
    private final String functionName;
    private final List<WarpExpression> arguments;
    private final Type type;

    @JsonCreator
    public WarpCall(@JsonProperty("functionName") String functionName,
            @JsonProperty("arguments") List<WarpExpression> arguments,
            @JsonProperty("type") Type type)
    {
        this.functionName = requireNonNull(functionName);
        this.arguments = requireNonNull(arguments);
        this.type = requireNonNull(type);
    }

    @JsonProperty
    public String getFunctionName()
    {
        return functionName;
    }

    @JsonProperty
    public List<WarpExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public List<? extends WarpExpression> getChildren()
    {
        return arguments;
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return "WarpCall{" +
                "functionName='" + functionName + '\'' +
                ", type=" + type +
                ", arguments=" + arguments +
                '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        WarpCall warpCall = (WarpCall) o;
        return Objects.equals(functionName, warpCall.getFunctionName()) &&
                Objects.equals(type, warpCall.getType()) &&
                Objects.equals(arguments, warpCall.getArguments());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionName, arguments, type);
    }
}
