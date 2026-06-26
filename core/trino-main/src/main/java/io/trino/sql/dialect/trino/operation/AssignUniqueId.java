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
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.operationmetadata.AssignUniqueIdOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.AssignUniqueIdOperationMetadata.NAME;
import static java.util.Objects.requireNonNull;

public final class AssignUniqueId
        extends TrinoOperation
{
    private final Result result;
    private final Value source;
    private final Attributes attributes;

    public AssignUniqueId(String resultName, Value source, Attributes sourceAttributes)
    {
        this(resultName, source, sourceAttributes, Attributes.empty());
    }

    public AssignUniqueId(String resultName, Value source, Attributes sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(source, "source is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(source.type()))) {
            throw new TrinoException(IR_ERROR, "source of AssignUniqueId operation must be of relation type");
        }
        this.source = source;

        List<Type> outputTypes = ImmutableList.<Type>builder()
                .addAll(relationRowType(trinoType(source.type())).getTypeParameters())
                .add(BIGINT)
                .build();
        this.result = new Result(resultName, irType(new MultisetType(RowType.anonymous(outputTypes))));

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(AssignUniqueIdOperationMetadata.deriveAttributes(Attributes.empty(), ImmutableList.of(sourceAttributes)));
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
        return ImmutableList.of(source);
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
        return "pretty assign_unique_id";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new AssignUniqueId(
                result.name(),
                newArgument,
                Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return Attributes.empty();
    }

    public Value source()
    {
        return source;
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitAssignUniqueId(this, context);
    }
}
