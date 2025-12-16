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
package io.trino.sql.dialect.trino.operation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;

import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata.TO_TYPE;
import static java.util.Objects.requireNonNull;

public final class Cast
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Map<AttributeKey, Object> attributes;

    public Cast(String resultName, Value input, Type type, Map<AttributeKey, Object> sourceAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(type, "type is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");

        this.result = new Result(resultName, irType(type));

        this.input = input;

        Map<AttributeKey, Object> operationAttributes = TO_TYPE.asMap(type);

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(CastOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(sourceAttributes)));

        this.attributes = attributes.buildOrThrow();
    }

    @Override
    public Result result()
    {
        return result;
    }

    @Override
    public List<Value> arguments()
    {
        return ImmutableList.of(input);
    }

    @Override
    public List<Region> regions()
    {
        return ImmutableList.of();
    }

    @Override
    public Map<AttributeKey, Object> attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "cast :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Cast(
                result.name(),
                newArgument,
                trinoType(result.type()),
                ImmutableMap.of());
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Cast(newName, input, trinoType(result.type()), ImmutableMap.of());
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(CastOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Value argument()
    {
        return input;
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitCast(this, context);
    }
}
