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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.PlanNodeIdAllocator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.ProjectNode;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.SystemSessionProperties.isUseSessionTimezoneForRenderingTimestamp;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.trino.sql.planner.plan.Patterns.output;
import static java.util.Objects.requireNonNull;

public class AddAtTimezoneOutput
        implements Rule<OutputNode>
{
    private static final String AT_TIMEZONE = "at_timezone";
    private static final Pattern<OutputNode> PATTERN = output()
            .matching(outputNode -> {
                PlanNode sourceNode = outputNode.getSource();
                if (sourceNode instanceof ProjectNode projectNode) {
                    return projectNode.getAssignments().entrySet().stream()
                            .anyMatch(entry -> (entry.getKey().type() instanceof TimestampWithTimeZoneType) && !isAtTimeZoneExpression(entry.getValue()));
                }
                return sourceNode.getOutputSymbols().stream()
                        .anyMatch(symbol -> symbol.type() instanceof TimestampWithTimeZoneType);
            });

    private final PlannerContext plannerContext;

    public AddAtTimezoneOutput(PlannerContext plannerContext)
    {
        this.plannerContext = requireNonNull(plannerContext, "plannerContext is null");
    }

    @Override
    public Pattern<OutputNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(OutputNode outputNode, Captures captures, Context context)
    {
        return addAtTimeZone(outputNode, context)
                .map(Result::ofPlanNode)
                .orElse(Result.empty());
    }

    private Optional<PlanNode> addAtTimeZone(OutputNode outputNode, Context context)
    {
        if (!isUseSessionTimezoneForRenderingTimestamp(context.getSession())) {
            return Optional.empty();
        }
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();
        TimeZoneKey sessionTimeZoneKey = context.getSession().getTimeZoneKey();
        Set<Symbol> permittedOutputs = ImmutableSet.copyOf(outputNode.getOutputSymbols());

        PlanNode sourceNode = outputNode.getSource();
        PlanNode planNode = context.getLookup().resolve(sourceNode);

        Assignments originalAssignments = Assignments.identity(sourceNode.getOutputSymbols().stream()
                .filter(permittedOutputs::contains)
                .collect(toImmutableList()));
        if (originalAssignments.size() != permittedOutputs.size()) {
            return Optional.empty();
        }

        Assignments assignmentsToUseForRewriteAssignments;

        // Check if at_timezone is already applied on the timestamp with timezone columns
        if (planNode instanceof ProjectNode projectNode) {
            boolean allMatch = projectNode.getAssignments().entrySet().stream()
                    .allMatch(entry -> (entry.getKey().type() instanceof TimestampWithTimeZoneType) == isAtTimeZoneExpression(entry.getValue()));
            if (allMatch) {
                return Optional.empty();
            }
            assignmentsToUseForRewriteAssignments = projectNode.getAssignments();
        }
        else {
            assignmentsToUseForRewriteAssignments = originalAssignments;
        }

        Optional<Assignments> assignmentsWithAtTimeZone = withAtTimeZoneAssignments(assignmentsToUseForRewriteAssignments, permittedOutputs, sessionTimeZoneKey);
        if (assignmentsWithAtTimeZone.isEmpty()) {
            return Optional.empty();
        }

        Assignments.Builder rewrittenAssignments = Assignments.builder();
        for (Map.Entry<Symbol, Expression> assignment : originalAssignments.entrySet()) {
            rewrittenAssignments.put(
                    assignment.getKey(),
                    inlineReferences(assignment.getValue(), assignmentsWithAtTimeZone.orElseThrow()));
        }

        ProjectNode newSourceNode = new ProjectNode(idAllocator.getNextId(), sourceNode, rewrittenAssignments.build());

        return Optional.of(new OutputNode(
                idAllocator.getNextId(),
                newSourceNode,
                outputNode.getColumnNames(),
                outputNode.getOutputSymbols()));
    }

    private Optional<Assignments> withAtTimeZoneAssignments(Assignments assignments, Set<Symbol> permittedOutputs, TimeZoneKey timeZoneKey)
    {
        Assignments.Builder atTimeZoneAssignments = Assignments.builder();
        boolean isTimestampWithTimezoneTypeFound = false;

        for (Symbol symbol : permittedOutputs) {
            if (symbol.type() instanceof TimestampWithTimeZoneType) {
                // When at_timezone function is already applied to symbol then don't reply another at_timezone function, instead use the existing one
                if (isAtTimeZoneExpression(assignments.get(symbol))) {
                    Call atTimeZoneExpression = (Call) assignments.get(symbol);
                    atTimeZoneAssignments.add(
                            new Assignments.Assignment(
                                    symbol,
                                    new Call(
                                            atTimeZoneExpression.function(),
                                            ImmutableList.of(symbol.toSymbolReference(), atTimeZoneExpression.arguments().getLast()))));
                }
                else {
                    ResolvedFunction atTimezoneFunction = plannerContext.getMetadata().resolveBuiltinFunction(AT_TIMEZONE, fromTypes(symbol.type(), createVarcharType(timeZoneKey.getId().length())));
                    atTimeZoneAssignments.add(
                            new Assignments.Assignment(
                                    symbol,
                                    new Call(
                                            atTimezoneFunction,
                                            ImmutableList.of(symbol.toSymbolReference(), new Constant(createVarcharType(timeZoneKey.getId().length()), utf8Slice(timeZoneKey.getId()))))));
                }
                isTimestampWithTimezoneTypeFound = true;
            }
        }

        if (!isTimestampWithTimezoneTypeFound) {
            return Optional.empty();
        }

        return Optional.of(atTimeZoneAssignments.build());
    }

    private static Expression inlineReferences(Expression expression, Assignments assignments)
    {
        Function<Symbol, Expression> mapping = symbol -> {
            Expression result = assignments.get(symbol);
            if (result != null) {
                return result;
            }
            return symbol.toSymbolReference();
        };

        return inlineSymbols(mapping, expression);
    }

    private static boolean isAtTimeZoneExpression(Expression expression)
    {
        return expression instanceof Call callExpression
                && callExpression.function().name().getFunctionName().equals(AT_TIMEZONE);
    }
}
