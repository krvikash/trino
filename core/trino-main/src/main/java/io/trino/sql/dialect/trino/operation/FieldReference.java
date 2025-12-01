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
import io.trino.spi.TrinoException;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.operationmetadata.FieldReferenceOperationMetadata;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;
import java.util.Map;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operationmetadata.FieldReferenceOperationMetadata.FIELD_INDEX;
import static io.trino.sql.dialect.trino.operationmetadata.FieldReferenceOperationMetadata.NAME;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class FieldReference
        extends TrinoOperation
{
    private final Result result;
    private final Value base;
    private final Map<AttributeKey, Object> attributes;

    public FieldReference(String resultName, Value base, Integer fieldIndex, Map<AttributeKey, Object> sourceAttributes)
    {
        this(resultName, base, fieldIndex, sourceAttributes, ImmutableMap.of());
    }

    public FieldReference(String resultName, Value base, Integer fieldIndex, Map<AttributeKey, Object> sourceAttributes, Map<AttributeKey, Object> enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(base, "base is null");
        requireNonNull(fieldIndex, "fieldIndex is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!(trinoType(base.type()) instanceof RowType baseRowType)) {
            throw new TrinoException(IR_ERROR, "input to the FieldReference operation must be of row type. actual: " + trinoType(base.type()).getDisplayName());
        }

        if (fieldIndex < 0 || fieldIndex >= baseRowType.getFields().size()) {
            throw new TrinoException(IR_ERROR, format("invalid field index: %s. expected value in range [0, %s]", fieldIndex, baseRowType.getFields().size() - 1));
        }

        this.result = new Result(resultName, irType(baseRowType.getFields().get(fieldIndex).getType()));

        this.base = base;

        Map<AttributeKey, Object> operationAttributes = FIELD_INDEX.asMap(fieldIndex);

        ImmutableMap.Builder<AttributeKey, Object> attributes = ImmutableMap.builder();
        attributes.putAll(operationAttributes);
        attributes.putAll(FieldReferenceOperationMetadata.deriveAttributes(operationAttributes, ImmutableList.of(sourceAttributes)));

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
        return ImmutableList.of(base);
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
        return "pretty field reference";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new FieldReference(
                result.name(),
                newArgument,
                FIELD_INDEX.getAttribute(attributes),
                ImmutableMap.of());
    }

    @Override
    public Operation withResultName(String newName)
    {
        return new FieldReference(newName, base, FIELD_INDEX.getAttribute(attributes), ImmutableMap.of());
    }

    @Override
    public Map<AttributeKey, Object> operationAttributes()
    {
        return filterAttributes(FieldReferenceOperationMetadata.OPERATION_ATTRIBUTES);
    }

    public Value base()
    {
        return base;
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitFieldReference(this, context);
    }
}
