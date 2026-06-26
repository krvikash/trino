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
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.CastOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

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
    private final Attributes attributes;

    public Cast(String resultName, Value input, Type type, Attributes sourceAttributes)
    {
        this(resultName, input, type, sourceAttributes, Attributes.empty());
    }

    public Cast(String resultName, Value input, Type type, Attributes sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(type, "type is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        this.result = new Result(resultName, irType(type));

        this.input = input;

        Attributes operationAttributes = TO_TYPE.asAttributes(type);

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(CastOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(sourceAttributes)));

        // TODO check if new attributes are compatible with existing ones. In particular, internal attributes must not change
        attributes.putAll(enforcedAttributes);
        this.attributes = attributes.buildKeepingLast();
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
    public Attributes attributes()
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
                Attributes.empty());
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Cast(newName, input, trinoType(result.type()), Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
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
