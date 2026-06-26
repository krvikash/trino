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
import io.trino.sql.dialect.trino.operationmetadata.ReturnOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.sql.dialect.ir.IrAttributeUtils.terminalOperation;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.operationmetadata.ReturnOperationMetadata.NAME;
import static java.util.Objects.requireNonNull;

public final class Return
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Attributes attributes;

    public Return(String resultName, Value input, Attributes sourceAttributes)
    {
        this(resultName, input, sourceAttributes, Attributes.empty());
    }

    public Return(String resultName, Value input, Attributes sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        this.result = new Result(resultName, input.type());

        this.input = input;

        Attributes operationAttributes = terminalOperation();

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(ReturnOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(sourceAttributes)));

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
        return "pretty return";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Return(
                result.name(),
                newArgument,
                Attributes.empty());
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Return(newName, input, Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return Attributes.empty();
    }

    public Value argument()
    {
        return input;
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitReturn(this, context);
    }
}
