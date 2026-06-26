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
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.TrinoDialect;
import io.trino.sql.dialect.trino.operationmetadata.RowOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.operationmetadata.RowOperationMetadata.NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class Row
        extends TrinoOperation
{
    private final Result result;
    private final List<Value> fields;
    private final Attributes attributes;

    public Row(String resultName, List<Value> fields, List<Attributes> sourceAttributes)
    {
        this(resultName, fields, sourceAttributes, Attributes.empty());
    }

    public Row(String resultName, List<Value> fields, List<Attributes> sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(fields, "fields is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        // fails if there are no fields
        Type resultType = RowType.anonymous(
                fields.stream()
                        .map(Value::type)
                        .map(TrinoDialect::trinoType)
                        .collect(toImmutableList()));

        this.result = new Result(resultName, irType(resultType));

        this.fields = ImmutableList.copyOf(fields);

        if (sourceAttributes.size() != fields.size()) {
            throw new TrinoException(IR_ERROR, format("the number of source attribute maps: %s does not match the number of arguments: %s", sourceAttributes.size(), fields.size()));
        }

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(RowOperationMetadata.deriveAttributes(Attributes.empty(), sourceAttributes));
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
        return fields;
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
        return "row :)";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        List<Value> newFields = new ArrayList<>(fields);
        newFields.set(index, newArgument);
        return new Row(
                result.name(),
                newFields,
                emptySourceAttributes(fields.size()));
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new Row(newName, fields, emptySourceAttributes(fields.size()));
    }

    @Override
    public Attributes operationAttributes()
    {
        return Attributes.empty();
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitRow(this, context);
    }
}
