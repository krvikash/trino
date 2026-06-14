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
import com.google.common.collect.ListMultimap;
import io.trino.matching.Captures;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operationmetadata.OutputOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.QueryOperationMetadata;
import io.trino.sql.dialect.trino.operationmetadata.ValuesOperationMetadata;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.OperationId;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.Type;
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
import io.trino.sql.planner.exploratory.rule.Result;
import io.trino.sql.planner.exploratory.rule.UpdateOperationAttributes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.newir.DialectRegistry.TESTING_DIALECT_REGISTRY;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.program;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.singleConstantRow;
import static io.trino.sql.planner.exploratory.MemoTestingHelper.valuesOfRows;
import static io.trino.sql.planner.exploratory.physical.dummy.DummyCost.ZERO;
import static org.assertj.core.api.Assertions.assertThat;

class TestPass
{
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

        assertThat(physicalModel.companion().previousCosts(QueryOperationMetadata.NAME))
                .containsExactly(Optional.empty(), Optional.of(ZERO));
        assertThat(optimizedProgram.root())
                .isInstanceOfSatisfying(Query.class, query ->
                        assertThat(query.query().operations())
                                .extracting(Operation::name)
                                .containsExactly(ValuesOperationMetadata.NAME, OutputOperationMetadata.NAME));
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
            return ZERO;
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
    }
}
