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
import io.trino.sql.dialect.trino.operationmetadata.ProjectOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.FormatOptions.PrintOptions;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Value;

import java.util.List;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.sql.dialect.trino.OperationValidationUtils.validateRowSelector;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.TypeConstraint.IS_RELATION;
import static io.trino.sql.dialect.trino.operationmetadata.ProjectOperationMetadata.NAME;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.isPruningAssignments;
import static java.util.Objects.requireNonNull;

public final class Project
        extends TrinoOperation
{
    private final Result result;
    private final Value input;
    private final Region assignments;
    private final Attributes attributes;

    public Project(String resultName, Value input, Block assignments, Attributes sourceAttributes)
    {
        this(resultName, input, assignments, sourceAttributes, Attributes.empty());
    }

    public Project(String resultName, Value input, Block assignments, Attributes sourceAttributes, Attributes enforcedAttributes)
    {
        super(TRINO, NAME);
        requireNonNull(resultName, "resultName is null");
        requireNonNull(input, "input is null");
        requireNonNull(assignments, "assignments is null");
        requireNonNull(sourceAttributes, "sourceAttributes is null");
        requireNonNull(enforcedAttributes, "enforcedAttributes is null");

        if (!IS_RELATION.test(trinoType(input.type()))) {
            throw new TrinoException(IR_ERROR, "input to the Project operation must be of relation type");
        }
        this.input = input;

        validateRowSelector(assignments, relationRowType(trinoType(input.type())), "invalid assignments for Project operation");
        this.assignments = singleBlockRegion(assignments);

        Type resultType;
        if (trinoType(assignments.getReturnedType()).equals(EMPTY_ROW)) {
            resultType = new MultisetType(EMPTY_ROW);
        }
        else {
            resultType = new MultisetType((RowType) trinoType(assignments.getReturnedType()));
        }
        this.result = new Result(resultName, irType(resultType));

        Attributes.Builder attributes = Attributes.builder();
        attributes.putAll(ProjectOperationMetadata.deriveAttributes(Attributes.empty(), ImmutableList.of(sourceAttributes, assignments.getTerminalOperation().attributes())));
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
        return ImmutableList.of(assignments);
    }

    @Override
    public Attributes attributes()
    {
        return attributes;
    }

    @Override
    public String prettyPrint(int indentLevel, PrintOptions printOptions)
    {
        return "pretty project";
    }

    @Override
    public Operation withArgument(Value newArgument, int index)
    {
        validateArgument(newArgument, index);
        return new Project(
                result.name(),
                newArgument,
                assignments.getOnlyBlock(),
                Attributes.empty());
    }

    @Override
    public Attributes operationAttributes()
    {
        return Attributes.empty();
    }

    public Block assignments()
    {
        return assignments.getOnlyBlock();
    }

    public boolean isPruning()
    {
        return isPruningAssignments(assignments());
    }

    @Override
    public <R, C> R accept(TrinoOperationVisitor<R, C> visitor, C context)
    {
        return visitor.visitProject(this, context);
    }
}
