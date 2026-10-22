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
package io.trino.sql.planner.exploratory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ListMultimap;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operationmetadata.OutputOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ProjectOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.QueryOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.PropertyDomain;
import io.trino.sql.planner.exploratory.physical.RequirementDomain;
import io.trino.sql.planner.exploratory.physical.XOCompanionDialect;
import io.trino.sql.planner.exploratory.physical.dummy.DummyCost;
import io.trino.sql.planner.exploratory.physical.dummy.DummyCostDomain;
import io.trino.sql.planner.exploratory.physical.dummy.DummyProperty;
import io.trino.sql.planner.exploratory.physical.dummy.DummyPropertyDomain;
import io.trino.sql.planner.exploratory.physical.dummy.DummyRequirement;
import io.trino.sql.planner.exploratory.physical.dummy.DummyRequirementDomain;
import io.trino.sql.planner.exploratory.physical.dummy.DummyTrinoXOCompanionDialect;
import io.trino.sql.planner.exploratory.rule.Context;
import io.trino.sql.planner.exploratory.rule.NewMemoOperation;
import io.trino.sql.planner.exploratory.rule.OperationRule;
import io.trino.sql.planner.exploratory.rule.Patterns;
import io.trino.sql.planner.exploratory.rule.Result;
import io.trino.sql.planner.exploratory.rule.UpdateOperationAttributes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.XO_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.identityProjection;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.program;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.singleConstantRow;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.valuesOfRows;
import static io.trino.sql.planner.exploratory.physical.CostComparison.WORSE;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

class TestPass
{
    private static final long LARGE_LIMIT = 1_000_000;

    @Test
    public void testAppliesRulesUntilFixpoint()
    {
        Constant constant = new Constant("%constant", BIGINT, 1L);
        Row row = new Row("%row", List.of(constant.result()), List.of(Attributes.empty()));
        Return returnOperation = new Return("%return", row.result(), row.attributes());
        Values values = valuesOfRows(List.of(List.of(constant, row, returnOperation)));
        RecordingUpdateOperationAttributes rule = new RecordingUpdateOperationAttributes();

        Program optimizedProgram = Pass.initialize(program(values), List.of(rule), TESTING_DIALECT_REGISTRY, new RecordingPhysicalModel()).run();

        assertThat(rule.updates()).isPositive();
        assertThat(optimizedProgram.root()).isInstanceOf(Query.class);
    }

    @Test
    public void testPreviousCostPassedWhenAlternativeIsRefreshed()
    {
        Values values = valuesOfRows(List.of(singleConstantRow("", BIGINT, 1L)));
        RecordingPhysicalModel physicalModel = new RecordingPhysicalModel();

        Program optimizedProgram = Pass.initialize(program(values), List.of(), TESTING_DIALECT_REGISTRY, physicalModel).run();

        // each refresh receives the cost returned by the previous costing of the same alternative
        List<DummyCost> costs = physicalModel.companion().costs(QueryOperationMetadata.NAME);
        assertThat(costs).hasSizeGreaterThan(1);
        ImmutableList.Builder<Optional<DummyCost>> expectedPreviousCosts = ImmutableList.builder();
        expectedPreviousCosts.add(Optional.empty());
        costs.subList(0, costs.size() - 1).forEach(cost -> expectedPreviousCosts.add(Optional.of(cost)));
        assertThat(physicalModel.companion().previousCosts(QueryOperationMetadata.NAME))
                .containsExactlyElementsOf(expectedPreviousCosts.build());
        assertThat(optimizedProgram.root())
                .isInstanceOfSatisfying(Query.class, query ->
                        assertThat(query.query().operations())
                                .extracting(Operation::name)
                                .containsExactly(ValuesOperationMetadata.NAME, OutputOperationMetadata.NAME));
    }

    @Test
    public void testDummyCostsAddUpToOperationCount()
    {
        Program program = program(valuesOfRows(List.of(singleConstantRow("", BIGINT, 1L))));
        long operationCount = operationCount(Memo.forProgram(program, TESTING_DIALECT_REGISTRY));
        RecordingPhysicalModel physicalModel = new RecordingPhysicalModel();

        Pass.initialize(program, List.of(), TESTING_DIALECT_REGISTRY, physicalModel).run();

        // without rules every group has a single operation, so the root costs one unit per operation
        assertThat(physicalModel.companion().costs(QueryOperationMetadata.NAME).getLast()).isEqualTo(new DummyCost(operationCount));
    }

    @Test
    public void testDummyCostMakesCycleWorse()
    {
        Values values = valuesOfRows(List.of(singleConstantRow("", BIGINT, 42L)));
        Project identityProject = identityProjection(values);
        IdentityProjectionToSource rule = new IdentityProjectionToSource();
        RecordingPhysicalModel physicalModel = new RecordingPhysicalModel();

        Program optimizedProgram = Pass.initialize(program(List.of(values, identityProject)), List.of(rule), TESTING_DIALECT_REGISTRY, physicalModel).run();

        // the rule's result is equal to the source Values, so Memo merges the two groups and the
        // projection becomes an alternative of its own source group: a cycle
        assertThat(rule.observedSelfLoop()).isTrue();
        // through the cycle the projection has a finite cost, strictly more than the Values in the same group
        DummyCost projectCost = physicalModel.companion().costs(ProjectOperationMetadata.NAME).getLast();
        assertThat(projectCost.isInfinite()).isFalse();
        assertThat(DummyCostDomain.INSTANCE.decide(projectCost, physicalModel.companion().costs(ValuesOperationMetadata.NAME).getLast()))
                .isEqualTo(WORSE);
        assertThat(optimizedProgram.root())
                .isInstanceOfSatisfying(Query.class, query ->
                        assertThat(query.query().operations())
                                .extracting(Operation::name)
                                .containsExactly(ValuesOperationMetadata.NAME, OutputOperationMetadata.NAME));
    }

    @Test
    public void testInitialMemoGrowthTokenLimitIncludesOperationArity()
    {
        Program program = program(valuesOfRows(List.of(singleConstantRow("", BIGINT, 1L))));
        Memo memo = Memo.forProgram(program, TESTING_DIALECT_REGISTRY);

        assertThat(memo.operationTokens()).isGreaterThan(operationCount(memo));
        assertTrinoExceptionThrownBy(() -> Pass.initialize(
                program,
                List.of(),
                TESTING_DIALECT_REGISTRY,
                new RecordingPhysicalModel(),
                new OptimizationLimits(memo.operationTokens() - 1, LARGE_LIMIT, LARGE_LIMIT, LARGE_LIMIT)))
                .hasErrorCode(XO_ERROR)
                .hasMessage(
                        "Memo growth tokens limit exceeded: limit %s, consumed 0, requested %s"
                                .formatted(memo.operationTokens() - 1, memo.operationTokens()));
    }

    @Test
    public void testOptimizationWorkLimit()
    {
        Program program = program(valuesOfRows(List.of(singleConstantRow("", BIGINT, 1L))));
        RecordingUpdateOperationAttributes rule = new RecordingUpdateOperationAttributes();

        assertTrinoExceptionThrownBy(() -> Pass.initialize(
                        program,
                        List.of(rule),
                        TESTING_DIALECT_REGISTRY,
                        new RecordingPhysicalModel(),
                        new OptimizationLimits(LARGE_LIMIT, 2, LARGE_LIMIT, LARGE_LIMIT))
                .run())
                .hasErrorCode(XO_ERROR)
                .hasMessage("Optimization work limit exceeded: limit 2, consumed 2, requested 1");
        assertThat(rule.updates()).isZero();
    }

    @Test
    public void testExtractionAttemptLimit()
    {
        Program program = program(valuesOfRows(List.of(singleConstantRow("", BIGINT, 1L))));

        assertTrinoExceptionThrownBy(() -> Pass.initialize(
                        program,
                        List.of(),
                        TESTING_DIALECT_REGISTRY,
                        new RecordingPhysicalModel(),
                        new OptimizationLimits(LARGE_LIMIT, LARGE_LIMIT, 0, LARGE_LIMIT))
                .run())
                .hasErrorCode(XO_ERROR)
                .hasMessage("Extraction attempts limit exceeded: limit 0, consumed 0, requested 1");
    }

    @Test
    public void testOutputOperationLimit()
    {
        Program program = program(valuesOfRows(List.of(singleConstantRow("", BIGINT, 1L))));

        assertTrinoExceptionThrownBy(() -> Pass.initialize(
                        program,
                        List.of(),
                        TESTING_DIALECT_REGISTRY,
                        new RecordingPhysicalModel(),
                        new OptimizationLimits(LARGE_LIMIT, LARGE_LIMIT, LARGE_LIMIT, 0))
                .run())
                .hasErrorCode(XO_ERROR)
                .hasMessage("Output operations limit exceeded: limit 0, consumed 0, requested 1");
    }

    private static long operationCount(Memo memo)
    {
        return memo.groups().values().stream()
                .mapToLong(group -> group.operations().size())
                .sum();
    }

    private static final class RecordingUpdateOperationAttributes
            extends UpdateOperationAttributes
    {
        private int updates;

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            Result<NewMemoOperation> result = super.apply(operation, captures, context);
            if (result.isPresent()) {
                updates++;
            }
            return result;
        }

        public int updates()
        {
            return updates;
        }
    }

    /**
     * Replaces a projection with its source operation. The test applies it only to an
     * identity projection over Values, where the result is equivalent to the projection.
     */
    private static final class IdentityProjectionToSource
            implements OperationRule
    {
        private boolean observedSelfLoop;

        @Override
        public Pattern<MemoOperation> pattern()
        {
            return Patterns.operation().matching(operation -> operation.operationId().name().equals(ProjectOperationMetadata.NAME));
        }

        @Override
        public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
        {
            MemoGroup source = context.getGroup(((GroupChild) operation.children().getFirst()).groupId());
            if (source.operations().contains(operation)) {
                observedSelfLoop = true;
            }
            return source.operations().stream()
                    .filter(sourceOperation -> sourceOperation.operationId().name().equals(ValuesOperationMetadata.NAME))
                    .findFirst()
                    .map(sourceOperation -> Result.of(NewMemoOperation.operationLike(sourceOperation)))
                    .orElseGet(Result::unchanged);
        }

        public boolean observedSelfLoop()
        {
            return observedSelfLoop;
        }
    }

    private static final class RecordingPhysicalModel
            implements PhysicalModel<DummyCost, DummyRequirement, DummyProperty>
    {
        private final RecordingCompanionDialect companion = new RecordingCompanionDialect();

        @Override
        public CostDomain<DummyCost> costs()
        {
            return DummyCostDomain.INSTANCE;
        }

        @Override
        public RequirementDomain<DummyRequirement> requirements()
        {
            return DummyRequirementDomain.INSTANCE;
        }

        @Override
        public PropertyDomain<DummyRequirement, DummyProperty> properties()
        {
            return DummyPropertyDomain.INSTANCE;
        }

        @Override
        public XOCompanionDialect<DummyCost, DummyRequirement, DummyProperty> companionDialect(Dialect dialect)
        {
            return companion;
        }

        public RecordingCompanionDialect companion()
        {
            return companion;
        }
    }

    private static final class RecordingCompanionDialect
            implements XOCompanionDialect<DummyCost, DummyRequirement, DummyProperty>
    {
        private final DummyTrinoXOCompanionDialect delegate = DummyTrinoXOCompanionDialect.INSTANCE;
        private final ListMultimap<String, Optional<DummyCost>> previousCosts = ArrayListMultimap.create();
        private final ListMultimap<String, DummyCost> costs = ArrayListMultimap.create();

        @Override
        public DummyCost getCost(
                OperationId operationId,
                Attributes inherentOperationAttributes,
                Attributes derivedResultAttributes,
                List<Attributes> derivedChildAttributes,
                List<DummyProperty> childProperties,
                List<DummyCost> childCosts,
                Optional<DummyCost> previousCost)
        {
            previousCosts.put(operationId.name(), previousCost);
            DummyCost cost = delegate.getCost(operationId, inherentOperationAttributes, derivedResultAttributes, derivedChildAttributes, childProperties, childCosts, previousCost);
            costs.put(operationId.name(), cost);
            return cost;
        }

        @Override
        public DummyProperty getProperty(
                OperationId operationId,
                Attributes inherentOperationAttributes,
                Attributes derivedResultAttributes,
                List<Attributes> derivedChildAttributes,
                List<DummyProperty> childProperties)
        {
            return delegate.getProperty(operationId, inherentOperationAttributes, derivedResultAttributes, derivedChildAttributes, childProperties);
        }

        @Override
        public List<List<DummyRequirement>> decomposeRequirement(
                OperationId operationId,
                Attributes inherentOperationAttributes,
                DummyRequirement requirement)
        {
            return delegate.decomposeRequirement(operationId, inherentOperationAttributes, requirement);
        }

        @Override
        public EnforcerPipeline<DummyCost, DummyRequirement, DummyProperty> getEnforcers(
                Type baseType,
                Attributes derivedBaseAttributes,
                DummyProperty baseProperty,
                DummyRequirement requirement,
                DialectRegistry dialectRegistry,
                PhysicalModel<DummyCost, DummyRequirement, DummyProperty> physicalModel)
        {
            return delegate.getEnforcers(baseType, derivedBaseAttributes, baseProperty, requirement, dialectRegistry, physicalModel);
        }

        public List<Optional<DummyCost>> previousCosts(String operationName)
        {
            return previousCosts.get(operationName);
        }

        public List<DummyCost> costs(String operationName)
        {
            return costs.get(operationName);
        }
    }
}
