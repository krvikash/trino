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
package io.trino.sql.planner.optimizations.ctereuse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.dialect.trino.Context;
import io.trino.sql.dialect.trino.ProgramBuilder;
import io.trino.sql.dialect.trino.ScalarProgramBuilder;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.IsNull;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.OperationAndIndex;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalContext;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.TraversalState;
import io.trino.sql.planner.optimizations.ctereuse.CteReuse.UnifiedStatesAndCheckpointMapping;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.IrExpressions.comparison;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.optimizations.ctereuse.AssignmentsUtils.getFullPassthroughFieldSelector;
import static io.trino.sql.planner.optimizations.ctereuse.BranchesToCheckpointsMapping.identityBranchToCheckpoint;
import static io.trino.sql.planner.optimizations.ctereuse.CteReuse.areStaticPredicatesPairwiseDisjoint;
import static io.trino.sql.planner.optimizations.ctereuse.PredicateUtils.truePredicate;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCteReuseCheckpointHeuristics
{
    private static final RowType INPUT_ROW_TYPE = anonymousRow(BIGINT, BIGINT);
    private static final Session SESSION = testSessionBuilder().build();

    @Test
    public void testStaticPredicateDisjointness()
    {
        assertThat(areStaticPredicatesPairwiseDisjoint(
                ImmutableList.of(equalsPredicate("first", 1), equalsPredicate("first", 2)),
                INPUT_ROW_TYPE,
                PLANNER_CONTEXT,
                SESSION))
                .isTrue();

        assertThat(areStaticPredicatesPairwiseDisjoint(
                ImmutableList.of(equalsPredicate("first", 1), equalsPredicate("first", 1)),
                INPUT_ROW_TYPE,
                PLANNER_CONTEXT,
                SESSION))
                .isFalse();

        // Domains on different fields do not prove whether the complete predicates overlap.
        assertThat(areStaticPredicatesPairwiseDisjoint(
                ImmutableList.of(equalsPredicate("first", 1), equalsPredicate("second", 2)),
                INPUT_ROW_TYPE,
                PLANNER_CONTEXT,
                SESSION))
                .isFalse();

        // Every pair must be disjoint, not only the first pair considered.
        assertThat(areStaticPredicatesPairwiseDisjoint(
                ImmutableList.of(equalsPredicate("first", 1), equalsPredicate("first", 2), equalsPredicate("first", 2)),
                INPUT_ROW_TYPE,
                PLANNER_CONTEXT,
                SESSION))
                .isFalse();
    }

    @Test
    public void testProjectCheckpointRequiresSharedComputation()
    {
        assertThat(mergeProjects(
                computedAssignment(0),
                computedAssignment(0)).requireCheckpoint())
                .isTrue();

        assertThat(mergeProjects(
                computedAssignment(0),
                computedAssignment(1)).requireCheckpoint())
                .isFalse();

        assertThat(mergeProjects(
                passthroughAssignment(0),
                passthroughAssignment(0)).requireCheckpoint())
                .isFalse();

        assertThat(mergeProjects(
                constantAssignment(1),
                constantAssignment(1)).requireCheckpoint())
                .isFalse();
    }

    private static Block equalsPredicate(String symbolName, long value)
    {
        Symbol first = new Symbol(BIGINT, "first");
        Symbol second = new Symbol(BIGINT, "second");
        Expression expression = comparison(
                PLANNER_CONTEXT.getMetadata(),
                SQL_STANDARD,
                EQUAL,
                new Reference(BIGINT, symbolName),
                new io.trino.sql.ir.Constant(BIGINT, value));

        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator();
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(INPUT_ROW_TYPE));
        Block.Builder builder = new Block.Builder(Optional.of("^predicate"), ImmutableList.of(parameter));
        expression.accept(
                new ScalarProgramBuilder(nameAllocator),
                new Context(
                        builder,
                        ImmutableMap.of(
                                first, new Context.RowField(parameter, 0),
                                second, new Context.RowField(parameter, 1))));
        Operation result = builder.recentOperation();
        builder.addOperation(new Return(nameAllocator.newName(), result.result(), result.attributes()));
        return builder.build();
    }

    private static UnifiedStatesAndCheckpointMapping mergeProjects(Block firstAssignments, Block secondAssignments)
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator();
        Values unifiedOperation = new Values(nameAllocator.newName(), INPUT_ROW_TYPE, ImmutableList.of());
        List<Project> projects = ImmutableList.of(firstAssignments, secondAssignments).stream()
                .map(assignments -> new Project(nameAllocator.newName(), unifiedOperation.result(), assignments, unifiedOperation.attributes()))
                .collect(toImmutableList());

        Map<Operation, Operation> operationToDownstream = new HashMap<>();
        ImmutableList.Builder<TraversalState> branches = ImmutableList.builder();
        for (Project project : projects) {
            Type projectRowType = relationRowType(trinoType(project.result().type()));
            Project downstream = new Project(
                    nameAllocator.newName(),
                    project.result(),
                    getFullPassthroughFieldSelector("^assignments", projectRowType, nameAllocator),
                    project.attributes());
            operationToDownstream.put(project, downstream);

            Block.Parameter predicateParameter = new Block.Parameter(nameAllocator.newName(), irType(INPUT_ROW_TYPE));
            Block.Parameter enforcedPredicateParameter = new Block.Parameter(nameAllocator.newName(), irType(INPUT_ROW_TYPE));
            TraversalContext context = new TraversalContext(
                    FieldMapping.identity(INPUT_ROW_TYPE),
                    ImmutableSet.of(),
                    truePredicate(Optional.of("^predicate"), ImmutableList.of(predicateParameter), nameAllocator),
                    truePredicate(Optional.of("^enforced_predicate"), ImmutableList.of(enforcedPredicateParameter), nameAllocator),
                    OptionalLong.empty());
            branches.add(new TraversalState(context, new OperationAndIndex(project, 0)));
        }

        return new ProjectMerger().mergeNextSingleGroupOperation(
                unifiedOperation,
                branches.build(),
                ImmutableList.of(),
                identityBranchToCheckpoint(projects.size()),
                operationToDownstream,
                nameAllocator,
                new HashMap<>(),
                PLANNER_CONTEXT.getMetadata(),
                SQL_STANDARD);
    }

    private static Block computedAssignment(int field)
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator();
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(INPUT_ROW_TYPE));
        FieldReference fieldReference = new FieldReference(nameAllocator.newName(), parameter, field, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        IsNull isNull = new IsNull(nameAllocator.newName(), fieldReference.result(), fieldReference.attributes());
        return assignmentBlock(parameter, ImmutableList.of(fieldReference, isNull), isNull);
    }

    private static Block passthroughAssignment(int field)
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator();
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(INPUT_ROW_TYPE));
        FieldReference fieldReference = new FieldReference(nameAllocator.newName(), parameter, field, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        return assignmentBlock(parameter, ImmutableList.of(fieldReference), fieldReference);
    }

    private static Block constantAssignment(long value)
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator();
        Block.Parameter parameter = new Block.Parameter(nameAllocator.newName(), irType(INPUT_ROW_TYPE));
        Constant constant = new Constant(nameAllocator.newName(), BIGINT, value);
        return assignmentBlock(parameter, ImmutableList.of(constant), constant);
    }

    private static Block assignmentBlock(Block.Parameter parameter, List<? extends Operation> operations, Operation result)
    {
        ProgramBuilder.ValueNameAllocator nameAllocator = new ProgramBuilder.ValueNameAllocator(100);
        Block.Builder builder = new Block.Builder(Optional.of("^assignments"), ImmutableList.of(parameter));
        operations.forEach(builder::addOperation);
        Row row = new Row(nameAllocator.newName(), ImmutableList.of(result.result()), ImmutableList.of(result.attributes()));
        builder.addOperation(row);
        builder.addOperation(new Return(nameAllocator.newName(), row.result(), row.attributes()));
        return builder.build();
    }
}
