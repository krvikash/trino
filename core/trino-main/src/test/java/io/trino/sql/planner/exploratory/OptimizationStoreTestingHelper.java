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

import com.google.common.collect.ImmutableList;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.planner.exploratory.Candidate.CandidateKey;
import io.trino.sql.planner.exploratory.GroupOptimizationState.StateKey;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterLineage;
import io.trino.sql.planner.exploratory.OperationImplementationAlternative.AlternativeKey;
import io.trino.sql.planner.exploratory.physical.CostDomain;
import io.trino.sql.planner.exploratory.physical.PhysicalModel;
import io.trino.sql.planner.exploratory.physical.PropertyDomain;
import io.trino.sql.planner.exploratory.physical.Requirement;
import io.trino.sql.planner.exploratory.physical.RequirementDomain;
import io.trino.sql.planner.exploratory.physical.XOCompanionDialect;
import io.trino.sql.planner.exploratory.physical.dummy.DummyCost;
import io.trino.sql.planner.exploratory.physical.dummy.DummyCostDomain;
import io.trino.sql.planner.exploratory.physical.dummy.DummyProperty;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_MEMO_OPERATION;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_OPERATION_ATTRIBUTE_KEY;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.TEST_OPERATION_ATTRIBUTE_OBJECT;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.program;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.singleConstantRow;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.valuesOfRows;
import static io.trino.sql.planner.exploratory.MemoTestingUtil.attributes;
import static io.trino.sql.planner.exploratory.OptimizationStoreTestingHelper.TestRequirement.UNDEFINED;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.UNKNOWN;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyProperty.SCALAR;

final class OptimizationStoreTestingHelper
{
    static final PhysicalModel<DummyCost, TestRequirement, DummyProperty> PHYSICAL_MODEL = new TestingPhysicalModel();

    private OptimizationStoreTestingHelper() {}

    static Memo baseMemo()
    {
        return Memo.forProgram(program(valuesOfRows(ImmutableList.of(singleConstantRow("", BIGINT, 42L)))), TESTING_DIALECT_REGISTRY);
    }

    static OptimizationStore<DummyCost, TestRequirement, DummyProperty> store(Memo memo)
    {
        return new OptimizationStore<>(memo, PHYSICAL_MODEL);
    }

    static StateKey<TestRequirement> stateKey(int groupId, TestRequirement requirement)
    {
        return StateKey.stateKey(groupId, requirement, PHYSICAL_MODEL.requirements());
    }

    static GroupOptimizationState<DummyCost, TestRequirement, DummyProperty> state(StateKey<TestRequirement> stateKey)
    {
        return GroupOptimizationState.initialize(stateKey, PHYSICAL_MODEL.costs());
    }

    static CandidateKey<TestRequirement> candidateKey(
            OperationImplementationAlternative.AlternativeKey<TestRequirement> alternativeKey,
            TestRequirement requirement)
    {
        return CandidateKey.candidateKey(alternativeKey, requirement, PHYSICAL_MODEL.requirements());
    }

    static AlternativeKey<TestRequirement> alternativeKey(MemoOperation operation, List<TestRequirement> childRequirements)
    {
        return AlternativeKey.alternativeKey(operation, childRequirements, PHYSICAL_MODEL.requirements());
    }

    static MemoOperation leafOperation()
    {
        return MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withOperationId(new OperationId(TEST_MEMO_OPERATION.operationId().name(), ImmutableList.of(), ImmutableList.of()))
                .withGroupParameterTypes(ImmutableList.of())
                .withChildren(ImmutableList.of())
                .withAttributes(attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT))
                .build();
    }

    static MemoOperation alternativeLeafOperation()
    {
        return MemoOperationBuilder.from(leafOperation())
                .withDialect("another_dialect")
                .build();
    }

    static MemoOperation operationWithChild(int childGroupId)
    {
        return operationWithChild(TEST_MEMO_OPERATION.dialect(), childGroupId);
    }

    static MemoOperation operationWithChild(String dialectName, int childGroupId)
    {
        return MemoOperationBuilder.from(TEST_MEMO_OPERATION)
                .withDialect(dialectName)
                .withOperationId(new OperationId(TEST_MEMO_OPERATION.operationId().name(), ImmutableList.of(TEST_MEMO_OPERATION.resultType()), ImmutableList.of()))
                .withGroupParameterTypes(ImmutableList.of())
                .withChildren(ImmutableList.of(new GroupChild(childGroupId, ParameterLineage.identity(0))))
                .withAttributes(attributes(TEST_OPERATION_ATTRIBUTE_KEY, TEST_OPERATION_ATTRIBUTE_OBJECT))
                .build();
    }

    static OperationImplementationAlternative<DummyCost, TestRequirement, DummyProperty> alternative(MemoOperation operation)
    {
        return alternative(operation, UNKNOWN);
    }

    static OperationImplementationAlternative<DummyCost, TestRequirement, DummyProperty> alternative(MemoOperation operation, DummyCost cost)
    {
        return OperationImplementationAlternative.alternative(
                operation,
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(),
                DummyProperty.UNKNOWN,
                cost,
                PHYSICAL_MODEL.requirements(),
                PHYSICAL_MODEL.properties());
    }

    static OperationImplementationAlternative<DummyCost, TestRequirement, DummyProperty> alternativeWithChild(MemoOperation operation)
    {
        return alternativeWithChild(operation, UNDEFINED);
    }

    static OperationImplementationAlternative<DummyCost, TestRequirement, DummyProperty> alternativeWithChild(MemoOperation operation, TestRequirement childRequirement)
    {
        return OperationImplementationAlternative.alternative(
                operation,
                ImmutableList.of(childRequirement),
                ImmutableList.of(Attributes.empty()),
                ImmutableList.of(UNKNOWN),
                DummyProperty.UNKNOWN,
                UNKNOWN,
                PHYSICAL_MODEL.requirements(),
                PHYSICAL_MODEL.properties());
    }

    static OperationImplementationAlternative<DummyCost, TestRequirement, DummyProperty> replacedAlternative(OperationImplementationAlternative<DummyCost, TestRequirement, DummyProperty> alternative)
    {
        return OperationImplementationAlternative.alternative(
                alternative.operation(),
                alternative.childRequirements(),
                alternative.derivedChildAttributes(),
                alternative.childCosts(),
                SCALAR,
                alternative.cost(),
                PHYSICAL_MODEL.requirements(),
                PHYSICAL_MODEL.properties());
    }

    static Candidate<DummyCost, TestRequirement, DummyProperty> candidate(OperationImplementationAlternative<DummyCost, TestRequirement, DummyProperty> alternative)
    {
        return new Candidate<>(
                alternative,
                EnforcerPipeline.empty(
                        alternative.operation().resultType(),
                        alternative.operation().attributes(),
                        alternative.providedProperty(),
                        TESTING_DIALECT_REGISTRY,
                        PHYSICAL_MODEL),
                PHYSICAL_MODEL.costs());
    }

    static WinnersTier<DummyCost, TestRequirement, DummyProperty> winners(Candidate<DummyCost, TestRequirement, DummyProperty> candidate)
    {
        WinnersTier<DummyCost, TestRequirement, DummyProperty> winners = emptyWinners();
        winners.add(candidate);
        return winners;
    }

    static WinnersTier<DummyCost, TestRequirement, DummyProperty> emptyWinners()
    {
        return WinnersTier.empty(PHYSICAL_MODEL.costs());
    }

    enum TestRequirement
            implements Requirement
    {
        UNDEFINED,
        OTHER,
    }

    private static final class TestingPhysicalModel
            implements PhysicalModel<DummyCost, TestRequirement, DummyProperty>
    {
        @Override
        public CostDomain<DummyCost> costs()
        {
            return DummyCostDomain.INSTANCE;
        }

        @Override
        public RequirementDomain<TestRequirement> requirements()
        {
            return TestingRequirementDomain.INSTANCE;
        }

        @Override
        public PropertyDomain<TestRequirement, DummyProperty> properties()
        {
            return TestingPropertyDomain.INSTANCE;
        }

        @Override
        public XOCompanionDialect<DummyCost, TestRequirement, DummyProperty> companionDialect(Dialect dialect)
        {
            throw new UnsupportedOperationException("companion dialect is not needed in optimization store tests");
        }
    }

    private enum TestingRequirementDomain
            implements RequirementDomain<TestRequirement>
    {
        INSTANCE;

        @Override
        public TestRequirement rootRequirement()
        {
            return UNDEFINED;
        }

        @Override
        public TestRequirement canonicalize(TestRequirement requirement)
        {
            return requirement;
        }
    }

    private enum TestingPropertyDomain
            implements PropertyDomain<TestRequirement, DummyProperty>
    {
        INSTANCE;

        @Override
        public DummyProperty scalar()
        {
            return SCALAR;
        }

        @Override
        public DummyProperty canonicalize(DummyProperty property)
        {
            return property;
        }

        @Override
        public boolean satisfies(DummyProperty property, TestRequirement requirement)
        {
            return true;
        }

        @Override
        public DummyProperty minimalSatisfying(TestRequirement requirement)
        {
            return DummyProperty.UNKNOWN;
        }
    }
}
