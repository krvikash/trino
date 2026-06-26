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
import io.trino.spi.TrinoException;
import io.trino.sql.dialect.trino.operationmetadata.NullIfOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.operationmetadata.NullIfOperationMetadata.NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class NullIf
        extends TrinoOperation
{
    private final Result result;
    private final Value first;
    private final Value second;
    private final Attributes attributes;

    public NullIf(String resultName, Value first, Value second, List<Attributes> sourceAttributes)
    {
        this(resultName, first, second, sourceAttributes, Attributes.empty());
    }

    public NullIf(String resultName, Value first, Value second, List<Attributes> sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(first, "first is null");
        requireNonNull(second, "second is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        // TODO: verify that first and second can be coerced to the same type
        this.result = new Result(resultName, first.type());

        this.first = first;

        this.second = second;

        if (sourceAttributes.size() != 2) {
            throw new TrinoException(IR_ERROR, format("the number of source attribute maps: %s does not match the number of arguments: 2", sourceAttributes.size()));
        }

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(NullIfOperationMetadata.deriveAttributes(Attributes.empty(), sourceAttributes));
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
        return ImmutableList.of(first, second);
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
        return "null_if :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new NullIf(
                result.name(),
                index == 0 ? newArgument : first,
                index == 1 ? newArgument : second,
                emptySourceAttributes(2));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new NullIf(newName, first, second, emptySourceAttributes(2));
    }

    @Override
    public Attributes operationAttributes()
    {
        return Attributes.empty();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitNullIf(this, context);
    }
}
