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
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.CallOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.CallOperationMetadata.NAME;
import static io.trino.sql.dialect.trino.operationmetadata.CallOperationMetadata.RESOLVED_FUNCTION;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Call
        extends TrinoOperation
{
    private final Result result;
    private final List<Value> arguments;
    private final Attributes attributes;

    public Call(String resultName, List<Value> arguments, ResolvedFunction function, List<Attributes> sourceAttributes)
    {
        this(resultName, arguments, function, sourceAttributes, Attributes.empty());
    }

    public Call(String resultName, List<Value> arguments, ResolvedFunction function, List<Attributes> sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(arguments, "arguments is null");
        requireNonNull(function, "function is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (function.signature().getArgumentTypes().size() != arguments.size()) {
            throw new TrinoException(IR_ERROR, format("expected %s arguments, found: %s", function.signature().getArgumentTypes().size(), arguments.size()));
        }
        for (int i = 0; i < arguments.size(); i++) {
            Type expectedType = function.signature().getArgumentType(i);
            Type actualType = trinoType(arguments.get(i).type());
            if (!expectedType.equals(actualType)) {
                throw new TrinoException(IR_ERROR, format("invalid argument type. expected: %s, actual: %s", expectedType, actualType));
            }
        }

        this.result = new Result(resultName, irType(function.signature().getReturnType()));

        this.arguments = ImmutableList.copyOf(arguments);

        if (sourceAttributes.size() != arguments.size()) {
            throw new TrinoException(IR_ERROR, format("the number of source attribute maps: %s does not match the number of arguments: %s", sourceAttributes.size(), arguments.size()));
        }

        Attributes operationAttributes = RESOLVED_FUNCTION.asAttributes(function);

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(CallOperationMetadata.deriveAttributes(operationAttributes, sourceAttributes));

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
        return arguments;
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
        return "call :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newArguments = new ArrayList<>(arguments);
        newArguments.set(index, newArgument);
        return new Call(
                result.name(),
                newArguments,
                RESOLVED_FUNCTION.getAttribute(attributes),
                emptySourceAttributes(arguments.size()));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Call(newName, arguments, RESOLVED_FUNCTION.getAttribute(attributes), emptySourceAttributes(arguments.size()));
    }

    @Override
    public Attributes operationAttributes()
    {
        return filterAttributes(CallOperationMetadata.OPERATION_ATTRIBUTES);
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }
}
