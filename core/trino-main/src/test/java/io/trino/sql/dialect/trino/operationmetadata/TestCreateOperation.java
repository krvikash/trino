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
package io.trino.sql.dialect.trino.operationmetadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.CatalogHandle;
import io.trino.connector.TestingColumnHandle;
import io.trino.cost.PlanNodeStatsAndCostSummary;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.function.OperatorType;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MultisetType;
import io.trino.spi.type.RowType;
import io.trino.sql.dialect.trino.operation.AggregateCall;
import io.trino.sql.dialect.trino.operation.Aggregation;
import io.trino.sql.dialect.trino.operation.Array;
import io.trino.sql.dialect.trino.operation.Between;
import io.trino.sql.dialect.trino.operation.Bind;
import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.dialect.trino.operation.Case;
import io.trino.sql.dialect.trino.operation.Cast;
import io.trino.sql.dialect.trino.operation.Coalesce;
import io.trino.sql.dialect.trino.operation.Comparison;
import io.trino.sql.dialect.trino.operation.Constant;
import io.trino.sql.dialect.trino.operation.CorrelatedJoin;
import io.trino.sql.dialect.trino.operation.DynamicFilterSource;
import io.trino.sql.dialect.trino.operation.Exchange;
import io.trino.sql.dialect.trino.operation.ExplainAnalyze;
import io.trino.sql.dialect.trino.operation.FieldReference;
import io.trino.sql.dialect.trino.operation.Filter;
import io.trino.sql.dialect.trino.operation.GroupId;
import io.trino.sql.dialect.trino.operation.In;
import io.trino.sql.dialect.trino.operation.IsNull;
import io.trino.sql.dialect.trino.operation.Join;
import io.trino.sql.dialect.trino.operation.Lambda;
import io.trino.sql.dialect.trino.operation.Limit;
import io.trino.sql.dialect.trino.operation.Logical;
import io.trino.sql.dialect.trino.operation.NullIf;
import io.trino.sql.dialect.trino.operation.Output;
import io.trino.sql.dialect.trino.operation.Project;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.dialect.trino.operation.Return;
import io.trino.sql.dialect.trino.operation.Row;
import io.trino.sql.dialect.trino.operation.Sort;
import io.trino.sql.dialect.trino.operation.Switch;
import io.trino.sql.dialect.trino.operation.TableScan;
import io.trino.sql.dialect.trino.operation.TopN;
import io.trino.sql.dialect.trino.operation.Values;
import io.trino.sql.dialect.trino.operation.Window;
import io.trino.sql.dialect.trino.operation.WindowFunctionCall;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.ConstantValue;
import io.trino.sql.dialect.trino.operationmetadata.TrinoAttributeMetadata.SortOrderList;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Block.Parameter;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Operation.AttributeKey;
import io.trino.sql.newir.Operation.Result;
import io.trino.sql.newir.Region;
import io.trino.sql.newir.Type;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.type.FunctionType;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.EmptyRowType.EMPTY_ROW;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.dialect.ir.IrDialect.IR;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.NON_DETERMINISTIC;
import static io.trino.sql.dialect.ir.IrDialect.Repeatability.NON_IDEMPOTENT;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.mapStatistics;
import static io.trino.sql.dialect.trino.RelationalProgramBuilder.relationRowType;
import static io.trino.sql.dialect.trino.TrinoDialect.TESTING_TRINO_DIALECT;
import static io.trino.sql.dialect.trino.TrinoDialect.TRINO;
import static io.trino.sql.dialect.trino.TrinoDialect.irType;
import static io.trino.sql.dialect.trino.TrinoDialect.trinoType;
import static io.trino.sql.dialect.trino.operation.Values.valuesWithoutFields;
import static io.trino.sql.dialect.trino.operationmetadata.AggregationOperationMetadata.AggregationStep.SINGLE;
import static io.trino.sql.dialect.trino.operationmetadata.ComparisonOperationMetadata.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.dialect.trino.operationmetadata.ComparisonOperationMetadata.ComparisonOperator.LESS_THAN;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeScope.REMOTE;
import static io.trino.sql.dialect.trino.operationmetadata.ExchangeOperationMetadata.ExchangeType.GATHER;
import static io.trino.sql.dialect.trino.operationmetadata.JoinOperationMetadata.DistributionType.REPLICATED;
import static io.trino.sql.dialect.trino.operationmetadata.LogicalOperationMetadata.LogicalOperator.AND;
import static io.trino.sql.dialect.trino.operationmetadata.TopNOperationMetadata.TopNStep.FINAL;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameBoundType.FOLLOWING;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameBoundType.PRECEDING;
import static io.trino.sql.dialect.trino.operationmetadata.WindowFunctionCallOperationMetadata.WindowFrameType.RANGE;
import static io.trino.sql.newir.Region.singleBlockRegion;
import static java.lang.Boolean.TRUE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestCreateOperation
{
    private static final Values VALUES_OPERATION = valuesOperation();
    private static final Type VALUES_OPERATION_ROW_TYPE = irType(relationRowType(trinoType(VALUES_OPERATION.result().type())));
    private static final Region SOME_REGION = singleBlockRegion(new Block(Optional.empty(), ImmutableList.of(), ImmutableList.of(new Return("%5", new Result("%4", irType(BOOLEAN)), ImmutableMap.of()))));
    private static final Parameter INPUT_ROW_PARAMETER = new Parameter(
            "%input_row",
            irType(anonymousRow(BIGINT, BOOLEAN)));
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    @Test
    public void testAggregateCallAndAggregation()
    {
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));

        // aggregate parameter
        Parameter aggregateParameter = new Parameter(
                "%10",
                VALUES_OPERATION.result().type());

        // aggregate argument
        Parameter argumentsParameter = new Parameter(
                "%12",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationArgument = new FieldReference("%13", argumentsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationArgument = new Row("%14", ImmutableList.of(fieldReferenceOperationArgument.result()), ImmutableList.of(fieldReferenceOperationArgument.attributes()));
        Return returnOperationArgument = new Return("%15", rowOperationArgument.result(), rowOperationArgument.attributes());
        Block argumentsBlock = new Block(
                Optional.of("^arguments"),
                ImmutableList.of(argumentsParameter),
                ImmutableList.of(
                        fieldReferenceOperationArgument,
                        rowOperationArgument,
                        returnOperationArgument));

        // aggregate filter
        Parameter filterParameter = new Parameter(
                "%16",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationFilter = new FieldReference("%17", filterParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationFilter = new Row("%18", ImmutableList.of(fieldReferenceOperationFilter.result()), ImmutableList.of(fieldReferenceOperationFilter.attributes()));
        Return returnOperationFilter = new Return("%19", rowOperationFilter.result(), rowOperationFilter.attributes());
        Block filterSelectorBlock = new Block(
                Optional.of("^filterSelector"),
                ImmutableList.of(filterParameter),
                ImmutableList.of(
                        fieldReferenceOperationFilter,
                        rowOperationFilter,
                        returnOperationFilter));

        // aggregate mask
        Parameter maskParameter = new Parameter(
                "%20",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantOperationMask = new Constant("%21", EMPTY_ROW, null);
        Return returnOperationMask = new Return("%22", constantOperationMask.result(), constantOperationMask.attributes());
        Block maskSelectorBlock = new Block(
                Optional.of("^maskSelector"),
                ImmutableList.of(maskParameter),
                ImmutableList.of(
                        constantOperationMask,
                        returnOperationMask));

        // aggregate ordering
        Parameter orderingParameter = new Parameter(
                "%23",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingA = new FieldReference("%24", orderingParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%25", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row(
                "%26",
                ImmutableList.of(fieldReferenceOperationOrderingA.result(), fieldReferenceOperationOrderingB.result()),
                ImmutableList.of(fieldReferenceOperationOrderingA.attributes(), fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%27", rowOperationOrdering.result(), rowOperationOrdering.attributes());
        Block orderingSelectorBlock = new Block(
                Optional.of("^orderingSelector"),
                ImmutableList.of(orderingParameter),
                ImmutableList.of(
                        fieldReferenceOperationOrderingA,
                        fieldReferenceOperationOrderingB,
                        rowOperationOrdering,
                        returnOperationOrdering));

        AggregateCall aggregateCallOperation = new AggregateCall(
                "%11",
                aggregateParameter,
                BIGINT,
                argumentsBlock,
                filterSelectorBlock,
                maskSelectorBlock,
                orderingSelectorBlock,
                Optional.of(new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST))),
                sumFunction,
                false,
                AggregateCallOperationMetadata.AggregationStep.SINGLE);

        Operation actualAggregateCallOperation = TESTING_TRINO_DIALECT.createOperation(
                AggregateCallOperationMetadata.NAME,
                "%11",
                ImmutableList.of(aggregateParameter),
                ImmutableList.of(
                        singleBlockRegion(argumentsBlock),
                        singleBlockRegion(filterSelectorBlock),
                        singleBlockRegion(maskSelectorBlock),
                        singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "aggregate_call:result_type"), BIGINT,
                        new AttributeKey(TRINO, "aggregate_call:sort_orders"), new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "aggregate_call:resolved_function"), sumFunction,
                        new AttributeKey(TRINO, "aggregate_call:distinct"), false,
                        new AttributeKey(TRINO, "aggregate_call:step"), AggregateCallOperationMetadata.AggregationStep.SINGLE));

        assertThat(actualAggregateCallOperation).isEqualTo(aggregateCallOperation);
        assertThat(actualAggregateCallOperation.result().type()).isEqualTo(irType(BIGINT));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                AggregateCallOperationMetadata.NAME,
                "%11",
                ImmutableList.of(),
                ImmutableList.of(
                        singleBlockRegion(argumentsBlock),
                        singleBlockRegion(filterSelectorBlock),
                        singleBlockRegion(maskSelectorBlock),
                        singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "aggregate_call:result_type"), BIGINT,
                        new AttributeKey(TRINO, "aggregate_call:sort_orders"), new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "aggregate_call:resolved_function"), sumFunction,
                        new AttributeKey(TRINO, "aggregate_call:distinct"), false,
                        new AttributeKey(TRINO, "aggregate_call:step"), AggregateCallOperationMetadata.AggregationStep.SINGLE)))
                .hasMessage("AggregateCall operation must have exactly one argument: the input group");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                AggregateCallOperationMetadata.NAME,
                "%11",
                ImmutableList.of(aggregateParameter),
                ImmutableList.of(
                        singleBlockRegion(argumentsBlock),

                        singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "aggregate_call:result_type"), BIGINT,
                        new AttributeKey(TRINO, "aggregate_call:sort_orders"), new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "aggregate_call:resolved_function"), sumFunction,
                        new AttributeKey(TRINO, "aggregate_call:distinct"), false,
                        new AttributeKey(TRINO, "aggregate_call:step"), AggregateCallOperationMetadata.AggregationStep.SINGLE)))
                .hasMessage("AggregateCall operation must have exactly four regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                AggregateCallOperationMetadata.NAME,
                "%11",
                ImmutableList.of(aggregateParameter),
                ImmutableList.of(
                        singleBlockRegion(argumentsBlock),
                        singleBlockRegion(filterSelectorBlock),
                        singleBlockRegion(maskSelectorBlock),
                        singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "aggregate_call:result_type"), BIGINT,
                        new AttributeKey(TRINO, "aggregate_call:sort_orders"), new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "aggregate_call:distinct"), false,
                        new AttributeKey(TRINO, "aggregate_call:step"), AggregateCallOperationMetadata.AggregationStep.SINGLE)))
                .hasMessage("function is null");

        // collecting aggregate functions in a row
        Row aggregatesRowOperation = new Row("%28", ImmutableList.of(aggregateCallOperation.result()), ImmutableList.of(aggregateCallOperation.attributes()));
        Return aggregatesReturnOperation = new Return("%29", aggregatesRowOperation.result(), aggregatesRowOperation.attributes());
        Block aggregateCallsBlock = new Block(
                Optional.of("^aggregates"),
                ImmutableList.of(aggregateParameter),
                ImmutableList.of(
                        aggregateCallOperation,
                        aggregatesRowOperation,
                        aggregatesReturnOperation));

        // grouping keys
        Parameter groupingKeysParameter = new Parameter(
                "%30",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationGroupingKeys = new FieldReference("%31", groupingKeysParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationGroupingKeys = new Row("%32", ImmutableList.of(fieldReferenceOperationGroupingKeys.result()), ImmutableList.of(fieldReferenceOperationGroupingKeys.attributes()));
        Return returnOperationGroupingKeys = new Return("%33", rowOperationGroupingKeys.result(), rowOperationGroupingKeys.attributes());
        Block groupingKeysSelectorBlock = new Block(
                Optional.of("^groupingKeysSelector"),
                ImmutableList.of(groupingKeysParameter),
                ImmutableList.of(
                        fieldReferenceOperationGroupingKeys,
                        rowOperationGroupingKeys,
                        returnOperationGroupingKeys));

        Aggregation aggregationOperation = new Aggregation(
                "%9",
                VALUES_OPERATION.result(),
                aggregateCallsBlock,
                groupingKeysSelectorBlock,
                1,
                ImmutableList.of(),
                OptionalInt.empty(),
                ImmutableList.of(0),
                SINGLE,
                true,
                VALUES_OPERATION.attributes());

        Operation actualAggregationOperation = TESTING_TRINO_DIALECT.createOperation(
                AggregationOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(aggregateCallsBlock),
                        singleBlockRegion(groupingKeysSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "aggregation:grouping_sets_count"), 1,
                        new AttributeKey(TRINO, "aggregation:global_grouping_sets"), ImmutableList.of(),
                        new AttributeKey(TRINO, "aggregation:pre_grouped_indexes"), ImmutableList.of(0),
                        new AttributeKey(TRINO, "aggregation:step"), SINGLE,
                        new AttributeKey(TRINO, "aggregation:input_reducing"), true,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false));
        assertThat(actualAggregationOperation).isEqualTo(aggregationOperation);
        assertThat(actualAggregationOperation.result().type()).isEqualTo(irType(new MultisetType(anonymousRow(BOOLEAN, BIGINT))));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                AggregationOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result(), VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(aggregateCallsBlock),
                        singleBlockRegion(groupingKeysSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "aggregation:grouping_sets_count"), 1,
                        new AttributeKey(TRINO, "aggregation:global_grouping_sets"), ImmutableList.of(),
                        new AttributeKey(TRINO, "aggregation:pre_grouped_indexes"), ImmutableList.of(0),
                        new AttributeKey(TRINO, "aggregation:step"), SINGLE,
                        new AttributeKey(TRINO, "aggregation:input_reducing"), true,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Aggregation operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                AggregationOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(aggregateCallsBlock),
                        singleBlockRegion(groupingKeysSelectorBlock),
                        singleBlockRegion(groupingKeysSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "aggregation:grouping_sets_count"), 1,
                        new AttributeKey(TRINO, "aggregation:global_grouping_sets"), ImmutableList.of(),
                        new AttributeKey(TRINO, "aggregation:pre_grouped_indexes"), ImmutableList.of(0),
                        new AttributeKey(TRINO, "aggregation:step"), SINGLE,
                        new AttributeKey(TRINO, "aggregation:input_reducing"), true,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Aggregation operation must have exactly two regions: one for aggregate calls and one for grouping keys selector");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                AggregationOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(aggregateCallsBlock),
                        singleBlockRegion(groupingKeysSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "aggregation:grouping_sets_count"), 1,
                        new AttributeKey(TRINO, "aggregation:global_grouping_sets"), ImmutableList.of(),
                        new AttributeKey(TRINO, "aggregation:pre_grouped_indexes"), ImmutableList.of(0),
                        new AttributeKey(TRINO, "aggregation:input_reducing"), true,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("step is null");
    }

    @Test
    public void testArray()
    {
        Constant constantOperation1 = new Constant("%0", BOOLEAN, true);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Constant constantOperation3 = new Constant("%2", BOOLEAN, false);
        Array arrayOperation = new Array(
                "%3",
                BOOLEAN,
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        Operation actualArrayOperation = TESTING_TRINO_DIALECT.createOperation(
                ArrayOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "array:element_type"), BOOLEAN,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualArrayOperation).isEqualTo(arrayOperation);
        assertThat(actualArrayOperation.result().type()).isEqualTo(irType(new ArrayType(BOOLEAN)));

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ArrayOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "array:element_type"), BOOLEAN,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Array operation does not have regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ArrayOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("elementType is null");
    }

    @Test
    public void testBetween()
    {
        Constant constantOperationValue = new Constant("%0", BIGINT, 0L);
        Constant constantOperationMin = new Constant("%1", BIGINT, 1L);
        Constant constantOperationMax = new Constant("%2", BIGINT, 2L);
        Between betweenOperation = new Between(
                "%3",
                constantOperationValue.result(),
                constantOperationMin.result(),
                constantOperationMax.result(),
                ImmutableList.of(constantOperationValue.attributes(), constantOperationMin.attributes(), constantOperationMax.attributes()));

        Operation actualBetweenOperation = TESTING_TRINO_DIALECT.createOperation(
                BetweenOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperationValue.result(), constantOperationMin.result(), constantOperationMax.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualBetweenOperation).isEqualTo(betweenOperation);
        assertThat(actualBetweenOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                BetweenOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperationValue.result(), constantOperationMin.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Between operation must have exactly three arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                BetweenOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperationValue.result(), constantOperationMin.result(), constantOperationMax.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Between operation does not have regions");
    }

    @Test
    public void testBind()
    {
        FieldReference fieldReferenceOperation1 = new FieldReference("%0", INPUT_ROW_PARAMETER, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);

        Parameter lambdaArgument = new Parameter("%2", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperation2 = new FieldReference("%3", lambdaArgument, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperation = new Return("%4", fieldReferenceOperation2.result(), fieldReferenceOperation2.attributes());
        Block lambdaBody = new Block(
                Optional.of("^lambda"),
                ImmutableList.of(lambdaArgument),
                ImmutableList.of(
                        fieldReferenceOperation2,
                        returnOperation));
        Lambda lambdaOperation = new Lambda(
                "%1",
                lambdaBody);

        Operation actualBindOperation = TESTING_TRINO_DIALECT.createOperation(
                LambdaOperationMetadata.NAME,
                "%1",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(lambdaBody)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualBindOperation).isEqualTo(lambdaOperation);
        assertThat(actualBindOperation.result().type()).isEqualTo(irType(new FunctionType(ImmutableList.of(BIGINT), BIGINT)));

        Bind bindOperation = new Bind(
                "%5",
                ImmutableList.of(fieldReferenceOperation1.result()),
                lambdaOperation.result(),
                ImmutableList.of(fieldReferenceOperation1.attributes(), lambdaOperation.attributes()));

        assertThat(TESTING_TRINO_DIALECT.createOperation(
                BindOperationMetadata.NAME,
                "%5",
                ImmutableList.of(fieldReferenceOperation1.result(), lambdaOperation.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .isEqualTo(bindOperation);

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                BindOperationMetadata.NAME,
                "%5",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Bind operation must have at least one argument");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                BindOperationMetadata.NAME,
                "%5",
                ImmutableList.of(fieldReferenceOperation1.result(), lambdaOperation.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Bind operation does not have regions");
    }

    @Test
    public void testCall()
    {
        // call without arguments
        ResolvedFunction randomFunction = FUNCTION_RESOLUTION.resolveFunction("random", fromTypes());
        Call callOperationWithoutArguments = new Call("%0", ImmutableList.of(), randomFunction, ImmutableList.of());

        Operation actualCallOperation = TESTING_TRINO_DIALECT.createOperation(
                CallOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "call:resolved_function"), randomFunction,
                        new AttributeKey(IR, "repeatability"), NON_DETERMINISTIC,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualCallOperation).isEqualTo(callOperationWithoutArguments);
        assertThat(actualCallOperation.result().type()).isEqualTo(irType(DOUBLE));

        // call with arguments
        ResolvedFunction addOperator = FUNCTION_RESOLUTION.resolveOperator(OperatorType.ADD, ImmutableList.of(BIGINT, BIGINT));
        Constant constantOperation1 = new Constant("%0", BIGINT, 1L);
        Constant constantOperation2 = new Constant("%1", BIGINT, 2L);
        Call callOperationWithArguments = new Call(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                addOperator,
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));

        Operation actualCallOperationWithArguments = TESTING_TRINO_DIALECT.createOperation(
                CallOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "call:resolved_function"), addOperator,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualCallOperationWithArguments).isEqualTo(callOperationWithArguments);
        assertThat(actualCallOperationWithArguments.result().type()).isEqualTo(irType(BIGINT));

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CallOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "call:resolved_function"), randomFunction,
                        new AttributeKey(IR, "repeatability"), NON_DETERMINISTIC,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Call operation does not have regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CallOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(IR, "repeatability"), NON_DETERMINISTIC,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("function is null");
    }

    @Test
    public void testCase()
    {
        Constant constantOperationWhen1 = new Constant("%0", BOOLEAN, true);
        Constant constantOperationWhen2 = new Constant("%1", BOOLEAN, false);
        Constant constantOperationThen1 = new Constant("%2", BIGINT, 0L);
        Constant constantOperationThen2 = new Constant("%3", BIGINT, 1L);
        Constant constantOperationDefault = new Constant("%4", BIGINT, 2L);
        Case caseOperation = new Case(
                "%5",
                ImmutableList.of(constantOperationWhen1.result(), constantOperationWhen2.result()),
                ImmutableList.of(constantOperationThen1.result(), constantOperationThen2.result()),
                constantOperationDefault.result(),
                ImmutableList.of(
                        constantOperationWhen1.attributes(),
                        constantOperationWhen2.attributes(),
                        constantOperationThen1.attributes(),
                        constantOperationThen2.attributes(),
                        constantOperationDefault.attributes()));

        Operation actualCaseOperation = TESTING_TRINO_DIALECT.createOperation(
                CaseOperationMetadata.NAME,
                "%5",
                ImmutableList.of(constantOperationWhen1.result(), constantOperationWhen2.result(), constantOperationThen1.result(), constantOperationThen2.result(), constantOperationDefault.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualCaseOperation).isEqualTo(caseOperation);
        assertThat(actualCaseOperation.result().type()).isEqualTo(irType(BIGINT));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CaseOperationMetadata.NAME,
                "%5",
                ImmutableList.of(constantOperationWhen1.result(), constantOperationWhen2.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Case operation must have at least three arguments");

        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CaseOperationMetadata.NAME,
                "%5",
                ImmutableList.of(constantOperationWhen1.result(), constantOperationWhen2.result(), constantOperationThen1.result(), constantOperationThen2.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Case operation must have odd number of arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CaseOperationMetadata.NAME,
                "%5",
                ImmutableList.of(constantOperationWhen1.result(), constantOperationWhen2.result(), constantOperationThen1.result(), constantOperationThen2.result(), constantOperationDefault.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Case operation does not have regions");
    }

    @Test
    public void testCast()
    {
        Constant constantOperation = new Constant("%0", SMALLINT, 1L);
        Cast castOperation = new Cast(
                "%1",
                constantOperation.result(),
                BIGINT,
                constantOperation.attributes());

        Operation actualCastOperation = TESTING_TRINO_DIALECT.createOperation(
                CastOperationMetadata.NAME,
                "%1",
                ImmutableList.of(constantOperation.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "cast:to_type"), BIGINT,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualCastOperation).isEqualTo(castOperation);
        assertThat(actualCastOperation.result().type()).isEqualTo(irType(BIGINT));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CastOperationMetadata.NAME,
                "%1",
                ImmutableList.of(constantOperation.result(), constantOperation.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "cast:to_type"), BIGINT,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Cast operation must have exactly one argument: the input value");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CastOperationMetadata.NAME,
                "%1",
                ImmutableList.of(constantOperation.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "cast:to_type"), BIGINT,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Cast operation does not have regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CastOperationMetadata.NAME,
                "%1",
                ImmutableList.of(constantOperation.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("type is null");
    }

    @Test
    public void testCoalesce()
    {
        Constant constantOperation1 = new Constant("%0", BIGINT, null);
        Constant constantOperation2 = new Constant("%1", BIGINT, null);
        Constant constantOperation3 = new Constant("%2", BIGINT, 1L);
        Coalesce coalesceOperation = new Coalesce(
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        Operation actualCoalesceOperation = TESTING_TRINO_DIALECT.createOperation(
                CoalesceOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualCoalesceOperation).isEqualTo(coalesceOperation);
        assertThat(actualCoalesceOperation.result().type()).isEqualTo(irType(BIGINT));

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CoalesceOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Coalesce operation does not have regions");
    }

    @Test
    public void testComparison()
    {
        Constant constantOperationLeft = new Constant("%0", BIGINT, 0L);
        Constant constantOperationRight = new Constant("%1", BIGINT, 1L);
        Comparison comparisonOperation = new Comparison(
                "%2",
                constantOperationLeft.result(),
                constantOperationRight.result(),
                GREATER_THAN,
                ImmutableList.of(constantOperationLeft.attributes(), constantOperationRight.attributes()));

        Operation actualComparisonOperation = TESTING_TRINO_DIALECT.createOperation(
                ComparisonOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperationLeft.result(), constantOperationRight.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "comparison:operator"), GREATER_THAN,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualComparisonOperation).isEqualTo(comparisonOperation);
        assertThat(actualComparisonOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ComparisonOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperationLeft.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "comparison:operator"), GREATER_THAN,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Comparison operation must have exactly two arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ComparisonOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperationLeft.result(), constantOperationRight.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "comparison:operator"), GREATER_THAN,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Comparison operation does not have regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ComparisonOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperationLeft.result(), constantOperationRight.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("comparisonOperator is null");
    }

    @Test
    public void testConstant()
    {
        Constant constantOperation = new Constant("%0", BOOLEAN, true);

        Operation actualConstantOperation = TESTING_TRINO_DIALECT.createOperation(
                ConstantOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "constant:value"), ConstantValue.of(BOOLEAN, true),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualConstantOperation).isEqualTo(constantOperation);
        assertThat(actualConstantOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ConstantOperationMetadata.NAME,
                "%0",
                ImmutableList.of(new Result("%1", irType(BOOLEAN))),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "constant:value"), ConstantValue.of(BOOLEAN, true),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Constant operation does not have arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ConstantOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "constant:value"), ConstantValue.of(BOOLEAN, true),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Constant operation does not have regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ConstantOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessageMatching(".*\"constantValue\" is null");
    }

    @Test
    public void testConstantNull()
    {
        Constant constantOperation = new Constant("%0", BOOLEAN, null);

        Operation actualConstantOperation = TESTING_TRINO_DIALECT.createOperation(
                ConstantOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "constant:value"), ConstantValue.asNull(BOOLEAN),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualConstantOperation).isEqualTo(constantOperation);
        assertThat(actualConstantOperation.result().type()).isEqualTo(irType(BOOLEAN));
    }

    @Test
    public void testCorrelatedJoin()
    {
        // correlation
        Parameter correlationParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationCorrelation = new FieldReference("%11", correlationParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationCorrelation = new Row("%12", ImmutableList.of(fieldReferenceOperationCorrelation.result()), ImmutableList.of(fieldReferenceOperationCorrelation.attributes()));
        Return returnOperationCorrelation = new Return("%13", rowOperationCorrelation.result(), rowOperationCorrelation.attributes());
        Block correlationBlock = new Block(
                Optional.of("^correlationSelector"),
                ImmutableList.of(correlationParameter),
                ImmutableList.of(
                        fieldReferenceOperationCorrelation,
                        rowOperationCorrelation,
                        returnOperationCorrelation));

        // subquery
        Parameter subqueryParameter = new Parameter(
                "%14",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationSubquery = new FieldReference("%16", subqueryParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationSubquery = new Row("%17", ImmutableList.of(fieldReferenceOperationSubquery.result()), ImmutableList.of(fieldReferenceOperationSubquery.attributes()));
        Return returnOperationSubqueryRow = new Return("%18", rowOperationSubquery.result(), rowOperationSubquery.attributes());
        Values valuesOperationSubquery = new Values(
                "%15",
                RowType.anonymous(ImmutableList.of(BOOLEAN)),
                ImmutableList.of(new Block(
                        Optional.of("^row"),
                        ImmutableList.of(),
                        ImmutableList.of(
                                fieldReferenceOperationSubquery,
                                rowOperationSubquery,
                                returnOperationSubqueryRow))));
        Return returnOperationSubquery = new Return("%19", valuesOperationSubquery.result(), valuesOperationSubquery.attributes());
        Block subqueryBlock = new Block(
                Optional.of("^subquery"),
                ImmutableList.of(subqueryParameter),
                ImmutableList.of(
                        valuesOperationSubquery,
                        returnOperationSubquery));

        // filter

        // input row
        Parameter firstFilterParameter = new Parameter(
                "%20",
                VALUES_OPERATION_ROW_TYPE);
        // subquery row
        Parameter secondFilterParameter = new Parameter(
                "%21",
                irType(anonymousRow(BOOLEAN)));
        FieldReference fieldReferenceOperationFilter = new FieldReference("%22", firstFilterParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Return returnOperationFilter = new Return("%23", fieldReferenceOperationFilter.result(), fieldReferenceOperationFilter.attributes());
        Block filterBlock = new Block(
                Optional.of("^filter"),
                ImmutableList.of(firstFilterParameter, secondFilterParameter),
                ImmutableList.of(
                        fieldReferenceOperationFilter,
                        returnOperationFilter));

        CorrelatedJoin correlatedJoinOperation = new CorrelatedJoin(
                "%9",
                VALUES_OPERATION.result(),
                correlationBlock,
                subqueryBlock,
                filterBlock,
                CorrelatedJoinOperationMetadata.JoinType.LEFT,
                VALUES_OPERATION.attributes());

        Operation actualCorrelatedJoinOperation = TESTING_TRINO_DIALECT.createOperation(
                CorrelatedJoinOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(correlationBlock),
                        singleBlockRegion(subqueryBlock),
                        singleBlockRegion(filterBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "correlated_join:type"), CorrelatedJoinOperationMetadata.JoinType.LEFT,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualCorrelatedJoinOperation).isEqualTo(correlatedJoinOperation);
        assertThat(actualCorrelatedJoinOperation.result().type()).isEqualTo(irType(new MultisetType(anonymousRow(BIGINT, BOOLEAN, BOOLEAN))));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CorrelatedJoinOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(
                        singleBlockRegion(correlationBlock),
                        singleBlockRegion(subqueryBlock),
                        singleBlockRegion(filterBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "correlated_join:type"), CorrelatedJoinOperationMetadata.JoinType.LEFT,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("CorrelatedJoin operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CorrelatedJoinOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(correlationBlock),
                        singleBlockRegion(filterBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "correlated_join:type"), CorrelatedJoinOperationMetadata.JoinType.LEFT,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("CorrelatedJoin operation must have exactly three regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                CorrelatedJoinOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(correlationBlock),
                        singleBlockRegion(subqueryBlock),
                        singleBlockRegion(filterBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("joinType is null");
    }

    @Test
    public void testDynamicFilterSource()
    {
        // dynamic filter targets
        Parameter dynamicFilterTargetsParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", dynamicFilterTargetsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationA = new FieldReference("%12", dynamicFilterTargetsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row(
                "%13",
                ImmutableList.of(fieldReferenceOperationB.result(), fieldReferenceOperationA.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), fieldReferenceOperationA.attributes()));
        Return returnOperation = new Return("%14", rowOperation.result(), rowOperation.attributes());

        Block dynamicFilterTargetSelectorBlock = new Block(
                Optional.of("^dynamicFilterTargetSelector"),
                ImmutableList.of(dynamicFilterTargetsParameter),
                ImmutableList.of(
                        fieldReferenceOperationB,
                        fieldReferenceOperationA,
                        rowOperation,
                        returnOperation));
        DynamicFilterSource dynamicFilterSourceOperation = new DynamicFilterSource(
                "%9",
                VALUES_OPERATION.result(),
                dynamicFilterTargetSelectorBlock,
                ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                VALUES_OPERATION.attributes());

        Operation actualDynamicFilterSourceOperation = TESTING_TRINO_DIALECT.createOperation(
                DynamicFilterSourceOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(dynamicFilterTargetSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "dynamic_filter_source:dynamic_filter_ids"), ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualDynamicFilterSourceOperation).isEqualTo(dynamicFilterSourceOperation);
        assertThat(actualDynamicFilterSourceOperation.result().type()).isEqualTo(VALUES_OPERATION.result().type());

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                DynamicFilterSourceOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(dynamicFilterTargetSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "dynamic_filter_source:dynamic_filter_ids"), ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("DynamicFilterSource operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                DynamicFilterSourceOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "dynamic_filter_source:dynamic_filter_ids"), ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("DynamicFilterSource operation must have exactly one region: the dynamic filter target selector");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                DynamicFilterSourceOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(dynamicFilterTargetSelectorBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("dynamicFilterIds is null");
    }

    @Test
    public void testExchange()
    {
        ConnectorPartitioningHandle testingPartitioningHandle = new ConnectorPartitioningHandle() {};

        // right source -- values with no rows
        Values rightSourceOperation = new Values(
                "%9",
                RowType.anonymous(ImmutableList.of(SMALLINT, BIGINT, BOOLEAN)),
                ImmutableList.of());
        Type rightRowType = irType(relationRowType(trinoType(rightSourceOperation.result().type())));

        // input field selectors for exchange
        Parameter leftInputsParameter = new Parameter(
                "%11",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftInputsA = new FieldReference("%12", leftInputsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationLeftInputsB = new FieldReference("%13", leftInputsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationLeftInputs = new Row(
                "%14",
                ImmutableList.of(fieldReferenceOperationLeftInputsA.result(), fieldReferenceOperationLeftInputsB.result()),
                ImmutableList.of(fieldReferenceOperationLeftInputsA.attributes(), fieldReferenceOperationLeftInputsB.attributes()));
        Return returnOperationLeftInputs = new Return("%15", rowOperationLeftInputs.result(), rowOperationLeftInputs.attributes());
        Block firstInputSelectorBlock = new Block(
                Optional.of("^inputSelector"),
                ImmutableList.of(leftInputsParameter),
                ImmutableList.of(
                        fieldReferenceOperationLeftInputsA,
                        fieldReferenceOperationLeftInputsB,
                        rowOperationLeftInputs,
                        returnOperationLeftInputs));

        Parameter rightInputsParameter = new Parameter(
                "%16",
                rightRowType);
        FieldReference fieldReferenceOperationRightInputsD = new FieldReference("%17", rightInputsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationRightInputsE = new FieldReference("%18", rightInputsParameter, 2, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationRightInputs = new Row(
                "%19",
                ImmutableList.of(fieldReferenceOperationRightInputsD.result(), fieldReferenceOperationRightInputsE.result()),
                ImmutableList.of(fieldReferenceOperationRightInputsD.attributes(), fieldReferenceOperationRightInputsE.attributes()));
        Return returnOperationRightInputs = new Return("%20", rowOperationRightInputs.result(), rowOperationRightInputs.attributes());
        Block secondInputSelectorBlock = new Block(
                Optional.of("^inputSelector"),
                ImmutableList.of(rightInputsParameter),
                ImmutableList.of(
                        fieldReferenceOperationRightInputsD,
                        fieldReferenceOperationRightInputsE,
                        rowOperationRightInputs,
                        returnOperationRightInputs));

        RowType exchangeOutputRowType = anonymousRow(BIGINT, BOOLEAN);

        // partitioning bound arguments
        Parameter boundArgumentsParameter = new Parameter(
                "%21",
                irType(exchangeOutputRowType));
        FieldReference fieldReferenceOperationF = new FieldReference("%22", boundArgumentsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationBoundArguments = new Row("%23", ImmutableList.of(fieldReferenceOperationF.result()), ImmutableList.of(fieldReferenceOperationF.attributes()));
        Return returnOperationBoundArguments = new Return("%24", rowOperationBoundArguments.result(), rowOperationBoundArguments.attributes());
        Block partitioningBoundArgumentsBlock = new Block(
                Optional.of("^boundArguments"),
                ImmutableList.of(boundArgumentsParameter),
                ImmutableList.of(
                        fieldReferenceOperationF,
                        rowOperationBoundArguments,
                        returnOperationBoundArguments));

        // order by
        Parameter orderByParameter = new Parameter(
                "%25",
                irType(exchangeOutputRowType));
        Constant constantOperationOrderBy = new Constant("%26", EMPTY_ROW, null);
        Return returnOperationOrderBy = new Return("%27", constantOperationOrderBy.result(), constantOperationOrderBy.attributes());
        Block orderingSelectorBlock = new Block(
                Optional.of("^orderingSelector"),
                ImmutableList.of(orderByParameter),
                ImmutableList.of(
                        constantOperationOrderBy,
                        returnOperationOrderBy));

        Exchange exchangeOperation = new Exchange(
                "%10",
                ImmutableList.of(VALUES_OPERATION.result(), rightSourceOperation.result()),
                ImmutableList.of(
                        firstInputSelectorBlock,
                        secondInputSelectorBlock),
                partitioningBoundArgumentsBlock,
                orderingSelectorBlock,
                GATHER,
                REMOTE,
                new PartitioningHandle(
                        Optional.of(CatalogHandle.fromId("bla:normal:1")),
                        Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                        testingPartitioningHandle),
                new ExchangeOperationMetadata.ConstantValues(new ConstantValue[] {null}),
                false,
                Optional.of(ImmutableList.of(5, 6, 7)),
                OptionalInt.empty(),
                OptionalInt.empty(),
                Optional.empty(),
                ImmutableList.of(VALUES_OPERATION.attributes(), rightSourceOperation.attributes()));

        Operation actualExchangeOperation = TESTING_TRINO_DIALECT.createOperation(
                ExchangeOperationMetadata.NAME,
                "%10",
                ImmutableList.of(VALUES_OPERATION.result(), rightSourceOperation.result()),
                ImmutableList.of(
                        singleBlockRegion(firstInputSelectorBlock),
                        singleBlockRegion(secondInputSelectorBlock),
                        singleBlockRegion(partitioningBoundArgumentsBlock),
                        singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "exchange:type"), GATHER,
                        new AttributeKey(TRINO, "exchange:scope"), REMOTE,
                        new AttributeKey(TRINO, "exchange:partitioning_handle"), new PartitioningHandle(
                                Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                testingPartitioningHandle),
                        new AttributeKey(TRINO, "exchange:constant_values"), new ExchangeOperationMetadata.ConstantValues(new ConstantValue[] {null}),
                        new AttributeKey(TRINO, "exchange:replicate_nulls_and_any"), false,
                        new AttributeKey(TRINO, "exchange:bucket_to_partition"), ImmutableList.of(5, 6, 7),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualExchangeOperation).isEqualTo(exchangeOperation);
        assertThat(actualExchangeOperation.result().type()).isEqualTo(VALUES_OPERATION.result().type());

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ExchangeOperationMetadata.NAME,
                "%10",
                ImmutableList.of(VALUES_OPERATION.result(), rightSourceOperation.result()),
                ImmutableList.of(
                        singleBlockRegion(firstInputSelectorBlock),
                        singleBlockRegion(secondInputSelectorBlock),
                        singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "exchange:type"), GATHER,
                        new AttributeKey(TRINO, "exchange:scope"), REMOTE,
                        new AttributeKey(TRINO, "exchange:partitioning_handle"), new PartitioningHandle(
                                Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                testingPartitioningHandle),
                        new AttributeKey(TRINO, "exchange:constant_values"), new ExchangeOperationMetadata.ConstantValues(new ConstantValue[] {null}),
                        new AttributeKey(TRINO, "exchange:replicate_nulls_and_any"), false,
                        new AttributeKey(TRINO, "exchange:bucket_to_partition"), ImmutableList.of(5, 6, 7),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("The number of regions Exchange operation must be equal to the number of arguments plus two: one for partitioning bound arguments and one for sorting keys");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ExchangeOperationMetadata.NAME,
                "%10",
                ImmutableList.of(VALUES_OPERATION.result(), rightSourceOperation.result()),
                ImmutableList.of(
                        singleBlockRegion(firstInputSelectorBlock),
                        singleBlockRegion(secondInputSelectorBlock),
                        singleBlockRegion(partitioningBoundArgumentsBlock),
                        singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "exchange:scope"), REMOTE,
                        new AttributeKey(TRINO, "exchange:partitioning_handle"), new PartitioningHandle(
                                Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                testingPartitioningHandle),
                        new AttributeKey(TRINO, "exchange:constant_values"), new ExchangeOperationMetadata.ConstantValues(new ConstantValue[] {null}),
                        new AttributeKey(TRINO, "exchange:replicate_nulls_and_any"), false,
                        new AttributeKey(TRINO, "exchange:bucket_to_partition"), ImmutableList.of(5, 6, 7),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("type is null");
    }

    @Test
    public void testExplainAnalyze()
    {
        Parameter fieldSelectorParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperation = new FieldReference("%11", fieldSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row("%12", ImmutableList.of(fieldReferenceOperation.result()), ImmutableList.of(fieldReferenceOperation.attributes()));
        Return returnOperation = new Return("%13", rowOperation.result(), rowOperation.attributes());
        Block fieldSelectorBlock = new Block(
                Optional.of("^inputFieldSelector"),
                ImmutableList.of(fieldSelectorParameter),
                ImmutableList.of(
                        fieldReferenceOperation,
                        rowOperation,
                        returnOperation));

        ExplainAnalyze explainAnalyzeOperation = new ExplainAnalyze(
                "%9",
                VALUES_OPERATION.result(),
                fieldSelectorBlock,
                true,
                VALUES_OPERATION.attributes());

        Operation actualExplainAnalyzeOperation = TESTING_TRINO_DIALECT.createOperation(
                ExplainAnalyzeOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(fieldSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "explain_analyze:verbose"), true,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualExplainAnalyzeOperation).isEqualTo(explainAnalyzeOperation);
        assertThat(actualExplainAnalyzeOperation.result().type()).isEqualTo(irType(new MultisetType(anonymousRow(VARCHAR))));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ExplainAnalyzeOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(fieldSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "explain_analyze:verbose"), true,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("ExplainAnalyze operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ExplainAnalyzeOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "explain_analyze:verbose"), true,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("ExplainAnalyze operation must have exactly one region: the field selector");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ExplainAnalyzeOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(fieldSelectorBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessageMatching(".*the return value of .* is null");
    }

    @Test
    public void testFieldReference()
    {
        Constant constantOperation1 = new Constant("%0", BIGINT, 0L);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Row rowOperation = new Row(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));
        FieldReference fieldReferenceOperation = new FieldReference(
                "%3",
                rowOperation.result(),
                0,
                rowOperation.attributes());

        Operation actualFieldReferenceOperation = TESTING_TRINO_DIALECT.createOperation(
                FieldReferenceOperationMetadata.NAME,
                "%3",
                ImmutableList.of(rowOperation.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "field_reference:index"), 0,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualFieldReferenceOperation).isEqualTo(fieldReferenceOperation);
        assertThat(actualFieldReferenceOperation.result().type()).isEqualTo(irType(BIGINT));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                FieldReferenceOperationMetadata.NAME,
                "%3",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "field_reference:index"), 0,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("FieldReference operation must have exactly one argument: the base row value");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                FieldReferenceOperationMetadata.NAME,
                "%3",
                ImmutableList.of(rowOperation.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "field_reference:index"), 0,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("FieldReference operation does not have regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                FieldReferenceOperationMetadata.NAME,
                "%3",
                ImmutableList.of(rowOperation.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("fieldIndex is null");
    }

    @Test
    public void testFilter()
    {
        Parameter predicateParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperation = new FieldReference("%11", predicateParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperation = new Constant("%12", BIGINT, 5L);
        Comparison comparisonOperation = new Comparison(
                "%13",
                fieldReferenceOperation.result(),
                constantOperation.result(),
                GREATER_THAN,
                ImmutableList.of(fieldReferenceOperation.attributes(), constantOperation.attributes()));
        Return returnOperation = new Return("%14", comparisonOperation.result(), comparisonOperation.attributes());
        Block predicateBlock = new Block(
                Optional.of("^predicate"),
                ImmutableList.of(predicateParameter),
                ImmutableList.of(
                        fieldReferenceOperation,
                        constantOperation,
                        comparisonOperation,
                        returnOperation));

        Filter filterOperation = new Filter(
                "%9",
                VALUES_OPERATION.result(),
                predicateBlock,
                VALUES_OPERATION.attributes());

        Operation actualFilterOperation = TESTING_TRINO_DIALECT.createOperation(
                FilterOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(predicateBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualFilterOperation).isEqualTo(filterOperation);
        assertThat(actualFilterOperation.result().type()).isEqualTo(VALUES_OPERATION.result().type());

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                FilterOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(predicateBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Filter operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                FilterOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Filter operation must have exactly one region: the predicate");
    }

    @Test
    public void testGroupId()
    {
        // grouping columns selector
        Parameter groupingColumnsSelectorParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceA = new FieldReference("%11", groupingColumnsSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceB = new FieldReference("%12", groupingColumnsSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceBAnother = new FieldReference("%13", groupingColumnsSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationGroupingColumns = new Row(
                "%14",
                ImmutableList.of(fieldReferenceA.result(), fieldReferenceB.result(), fieldReferenceBAnother.result()),
                ImmutableList.of(fieldReferenceA.attributes(), fieldReferenceB.attributes(), fieldReferenceBAnother.attributes()));
        Return returnOperationGroupingColumns = new Return("%15", rowOperationGroupingColumns.result(), rowOperationGroupingColumns.attributes());
        Block groupingColumnsSelectorBlock = new Block(
                Optional.of("^groupingColumnsSelector"),
                ImmutableList.of(groupingColumnsSelectorParameter),
                ImmutableList.of(
                        fieldReferenceA,
                        fieldReferenceB,
                        fieldReferenceBAnother,
                        rowOperationGroupingColumns,
                        returnOperationGroupingColumns));

        // aggregation arguments selector
        Parameter aggregationArgumentsSelectorParameter = new Parameter(
                "%16",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceAAnother = new FieldReference("%17", aggregationArgumentsSelectorParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationAggregationArguments = new Row("%18", ImmutableList.of(fieldReferenceAAnother.result()), ImmutableList.of(fieldReferenceAAnother.attributes()));
        Return returnOperationAggregationArguments = new Return("%19", rowOperationAggregationArguments.result(), rowOperationAggregationArguments.attributes());
        Block aggregationArgumentsSelectorBlock = new Block(
                Optional.of("^aggregationArgumentsSelector"),
                ImmutableList.of(aggregationArgumentsSelectorParameter),
                ImmutableList.of(
                        fieldReferenceAAnother,
                        rowOperationAggregationArguments,
                        returnOperationAggregationArguments));

        GroupId groupIdOperation = new GroupId(
                "%9",
                VALUES_OPERATION.result(),
                groupingColumnsSelectorBlock,
                aggregationArgumentsSelectorBlock,
                ImmutableList.of(ImmutableList.of(2, 0), ImmutableList.of(1, 0)),
                VALUES_OPERATION.attributes());

        Operation actualGroupIdOperation = TESTING_TRINO_DIALECT.createOperation(
                GroupIdOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(groupingColumnsSelectorBlock),
                        singleBlockRegion(aggregationArgumentsSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "group_id:grouping_sets"), ImmutableList.of(ImmutableList.of(2, 0), ImmutableList.of(1, 0)),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualGroupIdOperation).isEqualTo(groupIdOperation);
        assertThat(actualGroupIdOperation.result().type()).isEqualTo(irType(new MultisetType(anonymousRow(BOOLEAN, BIGINT, BOOLEAN, BIGINT, BIGINT))));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                GroupIdOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(
                        singleBlockRegion(groupingColumnsSelectorBlock),
                        singleBlockRegion(aggregationArgumentsSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "group_id:grouping_sets"), ImmutableList.of(ImmutableList.of(2, 0), ImmutableList.of(1, 0)),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("GroupId operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                GroupIdOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(aggregationArgumentsSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "group_id:grouping_sets"), ImmutableList.of(ImmutableList.of(2, 0), ImmutableList.of(1, 0)),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("GroupId operation must have exactly two regions: one for grouping columns selector and one for aggregation arguments selector");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                GroupIdOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(groupingColumnsSelectorBlock),
                        singleBlockRegion(aggregationArgumentsSelectorBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("groupingSets is null");
    }

    @Test
    public void testIn()
    {
        Constant constantOperationValue = new Constant("%0", BIGINT, 1L);
        Constant constantOperation1 = new Constant("%1", BIGINT, 0L);
        Constant constantOperation2 = new Constant("%2", BIGINT, 1L);
        Constant constantOperation3 = new Constant("%3", BIGINT, 2L);
        In inOperation = new In(
                "%4",
                constantOperationValue.result(),
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(constantOperationValue.attributes(), constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        Operation actualInOperation = TESTING_TRINO_DIALECT.createOperation(
                InOperationMetadata.NAME,
                "%4",
                ImmutableList.of(constantOperationValue.result(), constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualInOperation).isEqualTo(inOperation);
        assertThat(actualInOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                InOperationMetadata.NAME,
                "%4",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("In operation arguments cannot be empty");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                InOperationMetadata.NAME,
                "%4",
                ImmutableList.of(constantOperationValue.result(), constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("In operation does not have regions");
    }

    @Test
    public void testIsNull()
    {
        Constant constantOperation = new Constant("%0", BIGINT, null);
        IsNull isNullOperation = new IsNull("%1", constantOperation.result(), constantOperation.attributes());

        Operation actualIsNullOperation = TESTING_TRINO_DIALECT.createOperation(
                IsNullOperationMetadata.NAME,
                "%1",
                ImmutableList.of(constantOperation.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualIsNullOperation).isEqualTo(isNullOperation);
        assertThat(actualIsNullOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                IsNullOperationMetadata.NAME,
                "%1",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("IsNull operation must have exactly one argument: the input value");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                IsNullOperationMetadata.NAME,
                "%1",
                ImmutableList.of(constantOperation.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("IsNull operation does not have regions");
    }

    @Test
    public void testJoin()
    {
        // right source
        Constant constantOperation = new Constant("%10", BIGINT, 5L);
        Row rowOperation = new Row("%11", ImmutableList.of(constantOperation.result()), ImmutableList.of(constantOperation.attributes()));
        Return returnOperation = new Return("%12", rowOperation.result(), rowOperation.attributes());
        Values rightSourceOperation = new Values(
                "%9",
                RowType.anonymous(ImmutableList.of(BIGINT)),
                ImmutableList.of(
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation,
                                        rowOperation,
                                        returnOperation))));
        Type rightRowType = irType(relationRowType(trinoType(rightSourceOperation.result().type())));

        // left join criteria
        Parameter leftCriteriaParameter = new Parameter(
                "%14",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftCriteria = new FieldReference("%15", leftCriteriaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationLeftCriteria = new Row("%16", ImmutableList.of(fieldReferenceOperationLeftCriteria.result()), ImmutableList.of(fieldReferenceOperationLeftCriteria.attributes()));
        Return returnOperationLeftCriteria = new Return("%17", rowOperationLeftCriteria.result(), rowOperationLeftCriteria.attributes());
        Block leftCriteriaSelectorBlock = new Block(
                Optional.of("^leftCriteriaSelector"),
                ImmutableList.of(leftCriteriaParameter),
                ImmutableList.of(
                        fieldReferenceOperationLeftCriteria,
                        rowOperationLeftCriteria,
                        returnOperationLeftCriteria));

        // right join criteria
        Parameter rightCriteriaParameter = new Parameter(
                "%18",
                rightRowType);
        FieldReference fieldReferenceOperationRightCriteria = new FieldReference("%19", rightCriteriaParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationRightCriteria = new Row("%20", ImmutableList.of(fieldReferenceOperationRightCriteria.result()), ImmutableList.of(fieldReferenceOperationRightCriteria.attributes()));
        Return returnOperationRightCriteria = new Return("%21", rowOperationRightCriteria.result(), rowOperationRightCriteria.attributes());
        Block rightCriteriaSelectorBlock = new Block(
                Optional.of("^rightCriteriaSelector"),
                ImmutableList.of(rightCriteriaParameter),
                ImmutableList.of(
                        fieldReferenceOperationRightCriteria,
                        rowOperationRightCriteria,
                        returnOperationRightCriteria));

        // join filter
        Parameter leftFilterParameter = new Parameter(
                "%22",
                VALUES_OPERATION_ROW_TYPE);
        Parameter rightFilterParameter = new Parameter(
                "%23",
                rightRowType);
        // JoinNode has empty filter, so default filter is created: constant true
        Constant constantOperationFilter = new Constant("%24", BOOLEAN, true);
        Return returnOperationFilter = new Return("%25", constantOperationFilter.result(), constantOperationFilter.attributes());
        Block filterBlock = new Block(
                Optional.of("^filter"),
                ImmutableList.of(leftFilterParameter, rightFilterParameter),
                ImmutableList.of(
                        constantOperationFilter,
                        returnOperationFilter));

        // left outputs
        Parameter leftOutputsParameter = new Parameter(
                "%26",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationLeftOutputsA = new FieldReference("%27", leftOutputsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationLeftOutputsB = new FieldReference("%28", leftOutputsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationLeftOutputs = new Row(
                "%29",
                ImmutableList.of(fieldReferenceOperationLeftOutputsA.result(), fieldReferenceOperationLeftOutputsB.result()),
                ImmutableList.of(fieldReferenceOperationLeftOutputsA.attributes(), fieldReferenceOperationLeftOutputsB.attributes()));
        Return returnOperationLeftOutputs = new Return("%30", rowOperationLeftOutputs.result(), rowOperationLeftOutputs.attributes());
        Block leftOutputSelectorBlock = new Block(
                Optional.of("^leftOutputSelector"),
                ImmutableList.of(leftOutputsParameter),
                ImmutableList.of(
                        fieldReferenceOperationLeftOutputsA,
                        fieldReferenceOperationLeftOutputsB,
                        rowOperationLeftOutputs,
                        returnOperationLeftOutputs));

        // right outputs
        Parameter rightOutputsParameter = new Parameter(
                "%31",
                rightRowType);
        FieldReference fieldReferenceOperationRightOutputsC = new FieldReference("%32", rightOutputsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationRightOutputs = new Row("%33", ImmutableList.of(fieldReferenceOperationRightOutputsC.result()), ImmutableList.of(fieldReferenceOperationRightOutputsC.attributes()));
        Return returnOperationRightOutputs = new Return("%34", rowOperationRightOutputs.result(), rowOperationRightOutputs.attributes());
        Block rightOutputSelectorBlock = new Block(
                Optional.of("^rightOutputSelector"),
                ImmutableList.of(rightOutputsParameter),
                ImmutableList.of(
                        fieldReferenceOperationRightOutputsC,
                        rowOperationRightOutputs,
                        returnOperationRightOutputs));

        // dynamic filter targets
        Parameter dynamicFilterTargetsParameter = new Parameter(
                "%35",
                rightRowType);
        FieldReference fieldReferenceOperationDynamicFilterTargetsC1 = new FieldReference("%36", dynamicFilterTargetsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationDynamicFilterTargetsC2 = new FieldReference("%37", dynamicFilterTargetsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationDynamicFilterTargets = new Row(
                "%38",
                ImmutableList.of(fieldReferenceOperationDynamicFilterTargetsC1.result(), fieldReferenceOperationDynamicFilterTargetsC2.result()),
                ImmutableList.of(fieldReferenceOperationDynamicFilterTargetsC1.attributes(), fieldReferenceOperationDynamicFilterTargetsC2.attributes()));
        Return returnOperationDynamicFilterTargets = new Return("%39", rowOperationDynamicFilterTargets.result(), rowOperationDynamicFilterTargets.attributes());
        Block dynamicFilterTargetSelectorBlock = new Block(
                Optional.of("^dynamicFilterTargetSelector"),
                ImmutableList.of(dynamicFilterTargetsParameter),
                ImmutableList.of(
                        fieldReferenceOperationDynamicFilterTargetsC1,
                        fieldReferenceOperationDynamicFilterTargetsC2,
                        rowOperationDynamicFilterTargets,
                        returnOperationDynamicFilterTargets));

        PlanNodeStatsAndCostSummary statsAndCost = new PlanNodeStatsAndCostSummary(1, 2, 3, 4, 5);

        Join joinOperation = new Join(
                "%13",
                VALUES_OPERATION.result(),
                rightSourceOperation.result(),
                leftCriteriaSelectorBlock,
                rightCriteriaSelectorBlock,
                filterBlock,
                leftOutputSelectorBlock,
                rightOutputSelectorBlock,
                dynamicFilterTargetSelectorBlock,
                JoinOperationMetadata.JoinType.LEFT,
                false,
                Optional.of(REPLICATED),
                Optional.of(true),
                ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                Optional.of(statsAndCost),
                VALUES_OPERATION.attributes(),
                rightSourceOperation.attributes());

        Operation actualJoinOperation = TESTING_TRINO_DIALECT.createOperation(
                JoinOperationMetadata.NAME,
                "%13",
                ImmutableList.of(VALUES_OPERATION.result(), rightSourceOperation.result()),
                ImmutableList.of(
                        singleBlockRegion(leftCriteriaSelectorBlock),
                        singleBlockRegion(rightCriteriaSelectorBlock),
                        singleBlockRegion(filterBlock),
                        singleBlockRegion(leftOutputSelectorBlock),
                        singleBlockRegion(rightOutputSelectorBlock),
                        singleBlockRegion(dynamicFilterTargetSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "join:type"), JoinOperationMetadata.JoinType.LEFT,
                        new AttributeKey(TRINO, "join:may_skip_output_duplicates"), false,
                        new AttributeKey(TRINO, "join:distribution_type"), REPLICATED,
                        new AttributeKey(TRINO, "join:spillable"), true,
                        new AttributeKey(TRINO, "join:dynamic_filter_ids"), ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                        new AttributeKey(TRINO, "join:statistics_and_cost_summary"), statsAndCost,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualJoinOperation).isEqualTo(joinOperation);
        assertThat(actualJoinOperation.result().type()).isEqualTo(irType(new MultisetType(anonymousRow(BIGINT, BOOLEAN, BIGINT))));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                JoinOperationMetadata.NAME,
                "%13",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(leftCriteriaSelectorBlock),
                        singleBlockRegion(rightCriteriaSelectorBlock),
                        singleBlockRegion(filterBlock),
                        singleBlockRegion(leftOutputSelectorBlock),
                        singleBlockRegion(rightOutputSelectorBlock),
                        singleBlockRegion(dynamicFilterTargetSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "join:type"), JoinOperationMetadata.JoinType.LEFT,
                        new AttributeKey(TRINO, "join:may_skip_output_duplicates"), false,
                        new AttributeKey(TRINO, "join:distribution_type"), JoinOperationMetadata.DistributionType.REPLICATED,
                        new AttributeKey(TRINO, "join:spillable"), true,
                        new AttributeKey(TRINO, "join:dynamic_filter_ids"), ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                        new AttributeKey(TRINO, "join:statistics_and_cost_summary"), statsAndCost,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Join operation must have exactly two arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                JoinOperationMetadata.NAME,
                "%13",
                ImmutableList.of(VALUES_OPERATION.result(), rightSourceOperation.result()),
                ImmutableList.of(
                        singleBlockRegion(rightOutputSelectorBlock),
                        singleBlockRegion(dynamicFilterTargetSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "join:type"), JoinOperationMetadata.JoinType.LEFT,
                        new AttributeKey(TRINO, "join:may_skip_output_duplicates"), false,
                        new AttributeKey(TRINO, "join:distribution_type"), JoinOperationMetadata.DistributionType.REPLICATED,
                        new AttributeKey(TRINO, "join:spillable"), true,
                        new AttributeKey(TRINO, "join:dynamic_filter_ids"), ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                        new AttributeKey(TRINO, "join:statistics_and_cost_summary"), statsAndCost,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Join operation must have exactly six regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                JoinOperationMetadata.NAME,
                "%13",
                ImmutableList.of(VALUES_OPERATION.result(), rightSourceOperation.result()),
                ImmutableList.of(
                        singleBlockRegion(leftCriteriaSelectorBlock),
                        singleBlockRegion(rightCriteriaSelectorBlock),
                        singleBlockRegion(filterBlock),
                        singleBlockRegion(leftOutputSelectorBlock),
                        singleBlockRegion(rightOutputSelectorBlock),
                        singleBlockRegion(dynamicFilterTargetSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "join:may_skip_output_duplicates"), false,
                        new AttributeKey(TRINO, "join:distribution_type"), JoinOperationMetadata.DistributionType.REPLICATED,
                        new AttributeKey(TRINO, "join:spillable"), true,
                        new AttributeKey(TRINO, "join:dynamic_filter_ids"), ImmutableList.of("first_dynamic_filter", "second_dynamic_filter"),
                        new AttributeKey(TRINO, "join:statistics_and_cost_summary"), statsAndCost,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("joinType is null");
    }

    @Test
    public void testLambdaWithoutArguments()
    {
        Constant constantOperation = new Constant("%2", BIGINT, 5L);
        Return returnOperation = new Return("%3", constantOperation.result(), constantOperation.attributes());
        Block lambdaBlock = new Block(
                Optional.of("^lambda"),
                ImmutableList.of(new Parameter("%1", irType(EMPTY_ROW))),
                ImmutableList.of(
                        constantOperation,
                        returnOperation));

        Lambda lambdaOperation = new Lambda("%0", lambdaBlock);

        Operation actualLambdaOperation = TESTING_TRINO_DIALECT.createOperation(
                LambdaOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(lambdaBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualLambdaOperation).isEqualTo(lambdaOperation);
        assertThat(actualLambdaOperation.result().type()).isEqualTo(irType(new FunctionType(ImmutableList.of(), BIGINT)));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                LambdaOperationMetadata.NAME,
                "%0",
                ImmutableList.of(new Result("%1", irType(BOOLEAN))),
                ImmutableList.of(singleBlockRegion(lambdaBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Lambda operation does not have arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                LambdaOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Lambda operation must have exactly one region: the lambda body");
    }

    @Test
    public void testLambdaWithArgument()
    {
        Parameter lambdaArgument = new Parameter("%1", irType(anonymousRow(BIGINT)));
        FieldReference fieldReferenceOperationX = new FieldReference("%2", lambdaArgument, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperation = new Constant("%3", BIGINT, 0L);
        Comparison comparisonOperation = new Comparison(
                "%4",
                fieldReferenceOperationX.result(),
                constantOperation.result(),
                LESS_THAN,
                ImmutableList.of(fieldReferenceOperationX.attributes(), constantOperation.attributes()));
        Return returnOperation = new Return("%5", comparisonOperation.result(), comparisonOperation.attributes());
        Block lambdaBlock = new Block(
                Optional.of("^lambda"),
                ImmutableList.of(lambdaArgument),
                ImmutableList.of(
                        fieldReferenceOperationX,
                        constantOperation,
                        comparisonOperation,
                        returnOperation));

        Lambda lambdaOperation = new Lambda(
                "%0",
                lambdaBlock);

        Operation actualLambdaOperation = TESTING_TRINO_DIALECT.createOperation(
                LambdaOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(lambdaBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualLambdaOperation).isEqualTo(lambdaOperation);
        assertThat(actualLambdaOperation.result().type()).isEqualTo(irType(new FunctionType(ImmutableList.of(BIGINT), BOOLEAN)));
    }

    @Test
    public void testLimit()
    {
        Parameter orderingParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantNull = new Constant("%11", EMPTY_ROW, null);
        Return returnOperation = new Return("%12", constantNull.result(), constantNull.attributes());
        Block orderingSelectorBlock = new Block(
                Optional.of("^orderingSelector"),
                ImmutableList.of(orderingParameter),
                ImmutableList.of(
                        constantNull,
                        returnOperation));

        Limit limitOperation = new Limit(
                "%9",
                VALUES_OPERATION.result(),
                orderingSelectorBlock,
                Optional.empty(),
                5L,
                true,
                ImmutableList.of(),
                VALUES_OPERATION.attributes());

        Operation actualLimitOperation = TESTING_TRINO_DIALECT.createOperation(
                LimitOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "limit:count"), 5L,
                        new AttributeKey(TRINO, "limit:partial"), true,
                        new AttributeKey(TRINO, "limit:pre_sorted_indexes"), ImmutableList.of(),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), NON_IDEMPOTENT,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualLimitOperation).isEqualTo(limitOperation);
        assertThat(actualLimitOperation.result().type()).isEqualTo(VALUES_OPERATION.result().type());

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                LimitOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "limit:count"), 5L,
                        new AttributeKey(TRINO, "limit:partial"), true,
                        new AttributeKey(TRINO, "limit:pre_sorted_indexes"), ImmutableList.of(),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), NON_IDEMPOTENT,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Limit operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                LimitOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "limit:count"), 5L,
                        new AttributeKey(TRINO, "limit:partial"), true,
                        new AttributeKey(TRINO, "limit:pre_sorted_indexes"), ImmutableList.of(),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), NON_IDEMPOTENT,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Limit operation must have exactly one region: the ordering selector");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                LimitOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "limit:partial"), true,
                        new AttributeKey(TRINO, "limit:pre_sorted_indexes"), ImmutableList.of(),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), NON_IDEMPOTENT,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessageMatching(".*the return value of .* is null");
    }

    @Test
    public void testLimitWithTies()
    {
        Parameter orderingParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%11", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row("%12", ImmutableList.of(fieldReferenceOperationOrderingB.result()), ImmutableList.of(fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%13", rowOperationOrdering.result(), rowOperationOrdering.attributes());
        Block orderingSelectorBlock = new Block(
                Optional.of("^orderingSelector"),
                ImmutableList.of(orderingParameter),
                ImmutableList.of(
                        fieldReferenceOperationOrderingB,
                        rowOperationOrdering,
                        returnOperationOrdering));

        Limit limitOperation = new Limit(
                "%9",
                VALUES_OPERATION.result(),
                orderingSelectorBlock,
                Optional.of(new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST))),
                5L,
                false,
                ImmutableList.of(0),
                VALUES_OPERATION.attributes());

        Operation actualLimitOperation = TESTING_TRINO_DIALECT.createOperation(
                LimitOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "limit:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "limit:count"), 5L,
                        new AttributeKey(TRINO, "limit:partial"), false,
                        new AttributeKey(TRINO, "limit:pre_sorted_indexes"), ImmutableList.of(0),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualLimitOperation).isEqualTo(limitOperation);
        assertThat(actualLimitOperation.result().type()).isEqualTo(VALUES_OPERATION.result().type());

        // missing sort_orders attribute: fails in Limit operation constructor
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                LimitOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "limit:count"), 5L,
                        new AttributeKey(TRINO, "limit:partial"), false,
                        new AttributeKey(TRINO, "limit:pre_sorted_indexes"), ImmutableList.of(0),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("ordering fields and sort orders for limit do not match in size");
    }

    @Test
    public void testLogical()
    {
        Constant constantOperation1 = new Constant("%0", BOOLEAN, true);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Constant constantOperation3 = new Constant("%2", BOOLEAN, false);
        Logical logicalOperation = new Logical(
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                AND,
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes(), constantOperation3.attributes()));

        Operation actualLogicalOperation = TESTING_TRINO_DIALECT.createOperation(
                LogicalOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "logical:operator"), AND,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualLogicalOperation).isEqualTo(logicalOperation);
        assertThat(actualLogicalOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                LogicalOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperation1.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "logical:operator"), AND,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Logical operation must have at least two arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                LogicalOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "logical:operator"), AND,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Logical operation does not have regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                LogicalOperationMetadata.NAME,
                "%3",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result(), constantOperation3.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("logicalOperator is null");
    }

    @Test
    public void testNullIf()
    {
        Constant constantOperationFirst = new Constant("%0", BIGINT, 0L);
        Constant constantOperationSecond = new Constant("%1", SMALLINT, 1L);
        NullIf nullIfOperation = new NullIf(
                "%2",
                constantOperationFirst.result(),
                constantOperationSecond.result(),
                ImmutableList.of(constantOperationFirst.attributes(), constantOperationSecond.attributes()));

        Operation actualNullIfOperation = TESTING_TRINO_DIALECT.createOperation(
                NullIfOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperationFirst.result(), constantOperationSecond.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualNullIfOperation).isEqualTo(nullIfOperation);
        assertThat(actualNullIfOperation.result().type()).isEqualTo(irType(BIGINT));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                NullIfOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperationFirst.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("NullIf operation must have exactly two arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                NullIfOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperationFirst.result(), constantOperationSecond.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("NullIf operation does not have regions");
    }

    @Test
    public void testOutput()
    {
        Parameter fieldReferenceParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", fieldReferenceParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationA = new FieldReference("%12", fieldReferenceParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row(
                "%13",
                ImmutableList.of(fieldReferenceOperationB.result(), fieldReferenceOperationA.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), fieldReferenceOperationA.attributes()));
        Return returnOperation = new Return("%14", rowOperation.result(), rowOperation.attributes());
        Block fieldSelectorBlock = new Block(
                Optional.of("^outputFieldSelector"),
                ImmutableList.of(fieldReferenceParameter),
                ImmutableList.of(
                        fieldReferenceOperationB,
                        fieldReferenceOperationA,
                        rowOperation,
                        returnOperation));

        Output outputOperation = new Output(
                "%9",
                VALUES_OPERATION.result(),
                fieldSelectorBlock,
                ImmutableList.of("col_b", "col_a"),
                VALUES_OPERATION.attributes());

        Operation actualOutputOperation = TESTING_TRINO_DIALECT.createOperation(
                OutputOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(fieldSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "output:column_names"), ImmutableList.of("col_b", "col_a"),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), true));

        assertThat(actualOutputOperation).isEqualTo(outputOperation);
        assertThat(actualOutputOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                OutputOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(fieldSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "output:column_names"), ImmutableList.of("col_b", "col_a"),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), true)))
                .hasMessage("Output operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                OutputOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "output:column_names"), ImmutableList.of("col_b", "col_a"),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), true)))
                .hasMessage("Output operation must have exactly one region: the field selector");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                OutputOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(fieldSelectorBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), true)))
                .hasMessage("outputNames is null");
    }

    @Test
    public void testProject()
    {
        Parameter assignmentsParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", assignmentsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationA = new FieldReference("%12", assignmentsParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperation = new Constant("%13", BIGINT, 5L);
        Comparison comparisonOperation = new Comparison(
                "%14",
                fieldReferenceOperationA.result(),
                constantOperation.result(),
                GREATER_THAN,
                ImmutableList.of(fieldReferenceOperationA.attributes(), constantOperation.attributes()));
        Row rowOperation = new Row(
                "%15",
                ImmutableList.of(fieldReferenceOperationB.result(), comparisonOperation.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), comparisonOperation.attributes()));
        Return returnOperation = new Return("%16", rowOperation.result(), rowOperation.attributes());
        Block assignmentsBlock = new Block(
                Optional.of("^assignments"),
                ImmutableList.of(assignmentsParameter),
                ImmutableList.of(
                        fieldReferenceOperationB,
                        fieldReferenceOperationA,
                        constantOperation,
                        comparisonOperation,
                        rowOperation,
                        returnOperation));

        Project projectOperation = new Project(
                "%9",
                VALUES_OPERATION.result(),
                assignmentsBlock,
                VALUES_OPERATION.attributes());

        Operation actualProjectOperation = TESTING_TRINO_DIALECT.createOperation(
                ProjectOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(assignmentsBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualProjectOperation).isEqualTo(projectOperation);
        assertThat(actualProjectOperation.result().type()).isEqualTo(irType(new MultisetType(anonymousRow(BOOLEAN, BOOLEAN))));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ProjectOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(assignmentsBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Project operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ProjectOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Project operation must have exactly one region: the assignments");
    }

    @Test
    public void testQuery()
    {
        Parameter fieldReferenceParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationB = new FieldReference("%11", fieldReferenceParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationA = new FieldReference("%12", fieldReferenceParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperation = new Row(
                "%13",
                ImmutableList.of(fieldReferenceOperationB.result(), fieldReferenceOperationA.result()),
                ImmutableList.of(fieldReferenceOperationB.attributes(), fieldReferenceOperationA.attributes()));
        Return returnOperation = new Return("%14", rowOperation.result(), rowOperation.attributes());
        Block fieldSelectorBlock = new Block(
                Optional.of("^outputFieldSelector"),
                ImmutableList.of(fieldReferenceParameter),
                ImmutableList.of(
                        fieldReferenceOperationB,
                        fieldReferenceOperationA,
                        rowOperation,
                        returnOperation));

        Output outputOperation = new Output(
                "%9",
                VALUES_OPERATION.result(),
                fieldSelectorBlock,
                ImmutableList.of("col_b", "col_a"),
                VALUES_OPERATION.attributes());

        Block queryBlock = new Block(
                Optional.of("^query"),
                ImmutableList.of(),
                ImmutableList.of(
                        VALUES_OPERATION,
                        outputOperation));

        Query queryOperation = new Query("%query", queryBlock);

        Operation actualQueryOperation = TESTING_TRINO_DIALECT.createOperation(
                QueryOperationMetadata.NAME,
                "%query",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(queryBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), true));

        assertThat(actualQueryOperation).isEqualTo(queryOperation);
        assertThat(actualQueryOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                QueryOperationMetadata.NAME,
                "%query",
                ImmutableList.of(new Result("%1", irType(BOOLEAN))),
                ImmutableList.of(singleBlockRegion(queryBlock)),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), true)))
                .hasMessage("Query operation does not have arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                QueryOperationMetadata.NAME,
                "%query",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), true)))
                .hasMessage("Query operation must have exactly one region: the query");
    }

    @Test
    public void testReturn()
    {
        Return returnOperation = new Return(
                "%0",
                INPUT_ROW_PARAMETER,
                DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);

        Operation actualReturnOperation = TESTING_TRINO_DIALECT.createOperation(
                ReturnOperationMetadata.NAME,
                "%0",
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualReturnOperation).isEqualTo(returnOperation);
        assertThat(actualReturnOperation.result().type()).isEqualTo(irType(anonymousRow(BIGINT, BOOLEAN)));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ReturnOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Return operation must have exactly one argument");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ReturnOperationMetadata.NAME,
                "%0",
                ImmutableList.of(INPUT_ROW_PARAMETER),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Return operation does not have regions");
    }

    @Test
    public void testRow()
    {
        Constant constantOperation1 = new Constant("%0", BIGINT, 0L);
        Constant constantOperation2 = new Constant("%1", BOOLEAN, true);
        Row rowOperation = new Row(
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(constantOperation1.attributes(), constantOperation2.attributes()));

        Operation actualRowOperation = TESTING_TRINO_DIALECT.createOperation(
                RowOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualRowOperation).isEqualTo(rowOperation);
        assertThat(actualRowOperation.result().type()).isEqualTo(irType(anonymousRow(BIGINT, BOOLEAN)));

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                RowOperationMetadata.NAME,
                "%2",
                ImmutableList.of(constantOperation1.result(), constantOperation2.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Row operation does not have regions");
    }

    @Test
    public void testSort()
    {
        Parameter orderingParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%11", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row("%12", ImmutableList.of(fieldReferenceOperationOrderingB.result()), ImmutableList.of(fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%13", rowOperationOrdering.result(), rowOperationOrdering.attributes());
        Block orderingSelectorBlock = new Block(
                Optional.of("^orderingSelector"),
                ImmutableList.of(orderingParameter),
                ImmutableList.of(
                        fieldReferenceOperationOrderingB,
                        rowOperationOrdering,
                        returnOperationOrdering));

        Sort sortOperation = new Sort(
                "%9",
                VALUES_OPERATION.result(),
                orderingSelectorBlock,
                new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                false,
                VALUES_OPERATION.attributes());

        Operation actualSortOperation = TESTING_TRINO_DIALECT.createOperation(
                SortOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "sort:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "sort:partial"), false,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualSortOperation).isEqualTo(sortOperation);
        assertThat(actualSortOperation.result().type()).isEqualTo(VALUES_OPERATION.result().type());

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                SortOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "sort:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "sort:partial"), false,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Sort operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                SortOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "sort:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "sort:partial"), false,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Sort operation must have exactly one region: the ordering selector");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                SortOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "sort:partial"), false,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("sortOrders is null");
    }

    @Test
    public void testSwitch()
    {
        Constant constantOperationOperand = new Constant("%0", BIGINT, 0L);
        Constant constantOperationWhen1 = new Constant("%1", BIGINT, 1L);
        Constant constantOperationWhen2 = new Constant("%2", BIGINT, 2L);
        Constant constantOperationThen1 = new Constant("%3", BOOLEAN, true);
        Constant constantOperationThen2 = new Constant("%4", BOOLEAN, false);
        Constant constantOperationDefault = new Constant("%5", BOOLEAN, null);

        Switch switchOperation = new Switch(
                "%6",
                constantOperationOperand.result(),
                ImmutableList.of(constantOperationWhen1.result(), constantOperationWhen2.result()),
                ImmutableList.of(constantOperationThen1.result(), constantOperationThen2.result()),
                constantOperationDefault.result(),
                ImmutableList.of(
                        constantOperationOperand.attributes(),
                        constantOperationWhen1.attributes(),
                        constantOperationWhen2.attributes(),
                        constantOperationThen1.attributes(),
                        constantOperationThen2.attributes(),
                        constantOperationDefault.attributes()));

        Operation actualSwitchOperation = TESTING_TRINO_DIALECT.createOperation(
                SwitchOperationMetadata.NAME,
                "%6",
                ImmutableList.of(
                        constantOperationOperand.result(),
                        constantOperationWhen1.result(),
                        constantOperationWhen2.result(),
                        constantOperationThen1.result(),
                        constantOperationThen2.result(),
                        constantOperationDefault.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualSwitchOperation).isEqualTo(switchOperation);
        assertThat(actualSwitchOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                SwitchOperationMetadata.NAME,
                "%6",
                ImmutableList.of(
                        constantOperationOperand.result(),
                        constantOperationDefault.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Switch operation must have at least four arguments");

        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                SwitchOperationMetadata.NAME,
                "%6",
                ImmutableList.of(
                        constantOperationOperand.result(),
                        constantOperationWhen1.result(),
                        constantOperationWhen2.result(),
                        constantOperationThen1.result(),
                        constantOperationDefault.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Switch operation must have even number of arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                SwitchOperationMetadata.NAME,
                "%6",
                ImmutableList.of(
                        constantOperationOperand.result(),
                        constantOperationWhen1.result(),
                        constantOperationWhen2.result(),
                        constantOperationThen1.result(),
                        constantOperationThen2.result(),
                        constantOperationDefault.result()),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Switch operation does not have regions");
    }

    @Test
    public void testTableScan()
    {
        ConnectorTableHandle testingConnectorTableHandle = new ConnectorTableHandle() {};

        TableScan tableScanOperation = new TableScan(
                "%0",
                RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE),
                ImmutableList.of(new TestingColumnHandle("a_handle"), new TestingColumnHandle("b_handle")),
                TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                Optional.of(mapStatistics(PlanNodeStatsEstimate.unknown(), ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")))),
                false,
                Optional.of(TRUE));

        Operation actualTableScanOperation = TESTING_TRINO_DIALECT.createOperation(
                TableScanOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "table_scan:table_handle"), new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE),
                        new AttributeKey(TRINO, "table_scan:column_handles"), ImmutableList.of(new TestingColumnHandle("a_handle"), new TestingColumnHandle("b_handle")),
                        new AttributeKey(TRINO, "table_scan:constraint"), TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                        new AttributeKey(TRINO, "table_scan:statistics"), mapStatistics(PlanNodeStatsEstimate.unknown(), ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))),
                        new AttributeKey(TRINO, "table_scan:update_target"), false,
                        new AttributeKey(TRINO, "table_scan:use_connector_node_partitioning"), true,
                        new AttributeKey(TRINO, "table_scan:row_type"), RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualTableScanOperation).isEqualTo(tableScanOperation);
        assertThat(actualTableScanOperation.result().type()).isEqualTo(irType(new MultisetType(anonymousRow(BIGINT, BOOLEAN))));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                TableScanOperationMetadata.NAME,
                "%0",
                ImmutableList.of(new Result("%1", irType(BOOLEAN))),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "table_scan:table_handle"), new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE),
                        new AttributeKey(TRINO, "table_scan:column_handles"), ImmutableList.of(new TestingColumnHandle("a_handle"), new TestingColumnHandle("b_handle")),
                        new AttributeKey(TRINO, "table_scan:constraint"), TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                        new AttributeKey(TRINO, "table_scan:statistics"), mapStatistics(PlanNodeStatsEstimate.unknown(), ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))),
                        new AttributeKey(TRINO, "table_scan:update_target"), false,
                        new AttributeKey(TRINO, "table_scan:use_connector_node_partitioning"), true,
                        new AttributeKey(TRINO, "table_scan:row_type"), RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("TableScan operation does not have arguments");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                TableScanOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(SOME_REGION),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "table_scan:table_handle"), new TableHandle(CatalogHandle.fromId("bla:normal:1"), testingConnectorTableHandle, TestingConnectorTransactionHandle.INSTANCE),
                        new AttributeKey(TRINO, "table_scan:column_handles"), ImmutableList.of(new TestingColumnHandle("a_handle"), new TestingColumnHandle("b_handle")),
                        new AttributeKey(TRINO, "table_scan:constraint"), TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                        new AttributeKey(TRINO, "table_scan:statistics"), mapStatistics(PlanNodeStatsEstimate.unknown(), ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))),
                        new AttributeKey(TRINO, "table_scan:update_target"), false,
                        new AttributeKey(TRINO, "table_scan:use_connector_node_partitioning"), true,
                        new AttributeKey(TRINO, "table_scan:row_type"), RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("TableScan operation does not have regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                TableScanOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "table_scan:column_handles"), ImmutableList.of(new TestingColumnHandle("a_handle"), new TestingColumnHandle("b_handle")),
                        new AttributeKey(TRINO, "table_scan:constraint"), TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                        new AttributeKey(TRINO, "table_scan:statistics"), mapStatistics(PlanNodeStatsEstimate.unknown(), ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))),
                        new AttributeKey(TRINO, "table_scan:update_target"), false,
                        new AttributeKey(TRINO, "table_scan:use_connector_node_partitioning"), true,
                        new AttributeKey(TRINO, "table_scan:row_type"), RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("tableHandle is null");
    }

    @Test
    public void testTopN()
    {
        Parameter orderingParameter = new Parameter(
                "%10",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%11", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row("%12", ImmutableList.of(fieldReferenceOperationOrderingB.result()), ImmutableList.of(fieldReferenceOperationOrderingB.attributes()));
        Return returnOperationOrdering = new Return("%13", rowOperationOrdering.result(), rowOperationOrdering.attributes());
        Block orderingSelectorBlock = new Block(
                Optional.of("^orderingSelector"),
                ImmutableList.of(orderingParameter),
                ImmutableList.of(
                        fieldReferenceOperationOrderingB,
                        rowOperationOrdering,
                        returnOperationOrdering));

        TopN topNOperation = new TopN(
                "%9",
                VALUES_OPERATION.result(),
                orderingSelectorBlock,
                new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                10,
                FINAL,
                VALUES_OPERATION.attributes());

        Operation actualTopNOperation = TESTING_TRINO_DIALECT.createOperation(
                TopNOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "top_n:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "top_n:limit"), 10L,
                        new AttributeKey(TRINO, "top_n:step"), FINAL,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualTopNOperation).isEqualTo(topNOperation);
        assertThat(actualTopNOperation.result().type()).isEqualTo(VALUES_OPERATION.result().type());

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                TopNOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "top_n:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "top_n:limit"), 10L,
                        new AttributeKey(TRINO, "top_n:step"), FINAL,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("TopN operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                TopNOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "top_n:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "top_n:limit"), 10L,
                        new AttributeKey(TRINO, "top_n:step"), FINAL,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("TopN operation must have exactly one region: the ordering selector");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                TopNOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(singleBlockRegion(orderingSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "top_n:limit"), 10L,
                        new AttributeKey(TRINO, "top_n:step"), FINAL,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("sortOrders is null");
    }

    @Test
    public void testValues()
    {
        Constant constantOperation1Row1 = new Constant("%1", BIGINT, 1L);
        Constant constantOperation2Row1 = new Constant("%2", BOOLEAN, true);
        Row rowOperation1 = new Row(
                "%3",
                ImmutableList.of(constantOperation1Row1.result(), constantOperation2Row1.result()),
                ImmutableList.of(constantOperation1Row1.attributes(), constantOperation2Row1.attributes()));
        Return returnOperation1 = new Return("%4", rowOperation1.result(), rowOperation1.attributes());
        Block firstRowBlock = new Block(
                Optional.of("^row"),
                ImmutableList.of(),
                ImmutableList.of(
                        constantOperation1Row1,
                        constantOperation2Row1,
                        rowOperation1,
                        returnOperation1));

        Constant constantOperation1Row2 = new Constant("%5", BIGINT, 2L);
        Constant constantOperation2Row2 = new Constant("%6", BOOLEAN, false);
        Row rowOperation2 = new Row(
                "%7",
                ImmutableList.of(constantOperation1Row2.result(), constantOperation2Row2.result()),
                ImmutableList.of(constantOperation1Row2.attributes(), constantOperation2Row2.attributes()));
        Return returnOperation2 = new Return("%8", rowOperation2.result(), rowOperation2.attributes());
        Block secondRowBlock = new Block(
                Optional.of("^row"),
                ImmutableList.of(),
                ImmutableList.of(
                        constantOperation1Row2,
                        constantOperation2Row2,
                        rowOperation2,
                        returnOperation2));

        Values valuesOperation = new Values(
                "%0",
                RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                ImmutableList.of(
                        firstRowBlock,
                        secondRowBlock));

        Operation actualVlauesOperation = TESTING_TRINO_DIALECT.createOperation(
                ValuesOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(
                        singleBlockRegion(firstRowBlock),
                        singleBlockRegion(secondRowBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "values:cardinality"), 2L,
                        new AttributeKey(TRINO, "values:row_type"), trinoType(VALUES_OPERATION_ROW_TYPE),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualVlauesOperation).isEqualTo(valuesOperation);
        assertThat(actualVlauesOperation.result().type()).isEqualTo(VALUES_OPERATION.result().type());

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ValuesOperationMetadata.NAME,
                "%0",
                ImmutableList.of(new Result("%1", irType(BOOLEAN))),
                ImmutableList.of(
                        singleBlockRegion(firstRowBlock),
                        singleBlockRegion(secondRowBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "values:cardinality"), 2L,
                        new AttributeKey(TRINO, "values:row_type"), trinoType(VALUES_OPERATION.result().type()),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Values operation does not have arguments");
    }

    @Test
    public void testValuesEmptyRow()
    {
        Values valuesOperation = valuesWithoutFields("%0", 5);

        Operation actualValuesOperation = TESTING_TRINO_DIALECT.createOperation(
                ValuesOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "values:cardinality"), 5L,
                        new AttributeKey(TRINO, "values:row_type"), EMPTY_ROW,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualValuesOperation).isEqualTo(valuesOperation);
        assertThat(actualValuesOperation.result().type()).isEqualTo(irType(new MultisetType(EMPTY_ROW)));

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                ValuesOperationMetadata.NAME,
                "%0",
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "values:row_type"), EMPTY_ROW,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "safe"), true,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessageMatching(".*the return value of .* is null");
    }

    @Test
    public void testWindowFunctionCallAndWindow()
    {
        ResolvedFunction lagFunction = FUNCTION_RESOLUTION.resolveFunction("lag", fromTypes(BOOLEAN, BIGINT));

        // window functions parameter
        Parameter windowFunctionsParameter = new Parameter(
                "%10",
                VALUES_OPERATION.result().type());

        // window function arguments
        Parameter argumentsParameter = new Parameter(
                "%12",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationArgument = new FieldReference("%13", argumentsParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Constant constantOperationArgument = new Constant("%14", BIGINT, 5L);
        Row rowOperationArgument = new Row(
                "%15",
                ImmutableList.of(fieldReferenceOperationArgument.result(), constantOperationArgument.result()),
                ImmutableList.of(fieldReferenceOperationArgument.attributes(), constantOperationArgument.attributes()));
        Return returnOperationArgument = new Return("%16", rowOperationArgument.result(), rowOperationArgument.attributes());
        Block argumentsBlock = new Block(
                Optional.of("^arguments"),
                ImmutableList.of(argumentsParameter),
                ImmutableList.of(
                        fieldReferenceOperationArgument,
                        constantOperationArgument,
                        rowOperationArgument,
                        returnOperationArgument));

        // window function ordering
        Parameter functionOrderingParameter = new Parameter(
                "%17",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationFunctionOrderingA = new FieldReference("%18", functionOrderingParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationFunctionOrderingB = new FieldReference("%19", functionOrderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationFunctionOrdering = new Row(
                "%20",
                ImmutableList.of(fieldReferenceOperationFunctionOrderingA.result(), fieldReferenceOperationFunctionOrderingB.result()),
                ImmutableList.of(fieldReferenceOperationFunctionOrderingA.attributes(), fieldReferenceOperationFunctionOrderingB.attributes()));
        Return returnOperationFunctionOrdering = new Return("%21", rowOperationFunctionOrdering.result(), rowOperationFunctionOrdering.attributes());
        Block orderingSelectorBlock = new Block(
                Optional.of("^orderingSelector"),
                ImmutableList.of(functionOrderingParameter),
                ImmutableList.of(
                        fieldReferenceOperationFunctionOrderingA,
                        fieldReferenceOperationFunctionOrderingB,
                        rowOperationFunctionOrdering,
                        returnOperationFunctionOrdering));

        // frame start field
        Parameter frameStartFieldParameter = new Parameter(
                "%22",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantOperationFrameStart = new Constant("%23", EMPTY_ROW, null);
        Return returnOperationFrameStart = new Return("%24", constantOperationFrameStart.result(), constantOperationFrameStart.attributes());
        Block frameStartFieldSelectorBlock = new Block(
                Optional.of("^frameStartFieldSelector"),
                ImmutableList.of(frameStartFieldParameter),
                ImmutableList.of(
                        constantOperationFrameStart,
                        returnOperationFrameStart));

        // sort key for frame start
        Parameter sortKeyCoercedForFrameStartComparisonParameter = new Parameter(
                "%25",
                VALUES_OPERATION_ROW_TYPE);
        Constant constantOperationSortKeyStart = new Constant("%26", EMPTY_ROW, null);
        Return returnOperationSortKeyStart = new Return("%27", constantOperationSortKeyStart.result(), constantOperationSortKeyStart.attributes());
        Block sortKeyCoercedForFrameStartComparisonSelectorBlock = new Block(
                Optional.of("^sortKeyCoercedForFrameStartComparisonSelector"),
                ImmutableList.of(sortKeyCoercedForFrameStartComparisonParameter),
                ImmutableList.of(
                        constantOperationSortKeyStart,
                        returnOperationSortKeyStart));

        // frame end field
        Parameter frameEndFieldParameter = new Parameter(
                "%28",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationFrameEnd = new FieldReference("%29", frameEndFieldParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationFrameEnd = new Row("%30", ImmutableList.of(fieldReferenceOperationFrameEnd.result()), ImmutableList.of(fieldReferenceOperationFrameEnd.attributes()));
        Return returnOperationFrameEnd = new Return("%31", rowOperationFrameEnd.result(), rowOperationFrameEnd.attributes());
        Block frameEndFieldSelectorBlock = new Block(
                Optional.of("^frameEndFieldSelector"),
                ImmutableList.of(frameEndFieldParameter),
                ImmutableList.of(
                        fieldReferenceOperationFrameEnd,
                        rowOperationFrameEnd,
                        returnOperationFrameEnd));

        // sort key for frame end
        Parameter sortKeyCoercedForFrameEndComparisonParameter = new Parameter(
                "%32",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationSortKeyEnd = new FieldReference("%33", sortKeyCoercedForFrameEndComparisonParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationSortKeyEnd = new Row("%34", ImmutableList.of(fieldReferenceOperationSortKeyEnd.result()), ImmutableList.of(fieldReferenceOperationSortKeyEnd.attributes()));
        Return returnOperationSortKeyEnd = new Return("%35", rowOperationSortKeyEnd.result(), rowOperationSortKeyEnd.attributes());
        Block sortKeyCoercedForFrameEndComparisonSelectorBlock = new Block(
                Optional.of("^sortKeyCoercedForFrameEndComparisonSelector"),
                ImmutableList.of(sortKeyCoercedForFrameEndComparisonParameter),
                ImmutableList.of(
                        fieldReferenceOperationSortKeyEnd,
                        rowOperationSortKeyEnd,
                        returnOperationSortKeyEnd));

        WindowFunctionCall windowFunctionCallOperation = new WindowFunctionCall(
                "%11",
                windowFunctionsParameter,
                argumentsBlock,
                orderingSelectorBlock,
                frameStartFieldSelectorBlock,
                sortKeyCoercedForFrameStartComparisonSelectorBlock,
                frameEndFieldSelectorBlock,
                sortKeyCoercedForFrameEndComparisonSelectorBlock,
                lagFunction,
                Optional.of(new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST))),
                RANGE,
                PRECEDING,
                FOLLOWING,
                true,
                false);

        Operation actualWindowFunctionCallOperation = TESTING_TRINO_DIALECT.createOperation(
                WindowFunctionCallOperationMetadata.NAME,
                "%11",
                ImmutableList.of(windowFunctionsParameter),
                ImmutableList.of(
                        singleBlockRegion(argumentsBlock),
                        singleBlockRegion(orderingSelectorBlock),
                        singleBlockRegion(frameStartFieldSelectorBlock),
                        singleBlockRegion(sortKeyCoercedForFrameStartComparisonSelectorBlock),
                        singleBlockRegion(frameEndFieldSelectorBlock),
                        singleBlockRegion(sortKeyCoercedForFrameEndComparisonSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "window_function_call:resolved_function"), lagFunction,
                        new AttributeKey(TRINO, "window_function_call:sort_orders"), new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "window_function_call:frame_type"), RANGE,
                        new AttributeKey(TRINO, "window_function_call:frame_start_type"), PRECEDING,
                        new AttributeKey(TRINO, "window_function_call:frame_end_type"), FOLLOWING,
                        new AttributeKey(TRINO, "window_function_call:ignore_nulls"), true,
                        new AttributeKey(TRINO, "window_function_call:distinct"), false,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualWindowFunctionCallOperation).isEqualTo(windowFunctionCallOperation);
        assertThat(actualWindowFunctionCallOperation.result().type()).isEqualTo(irType(BOOLEAN));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                WindowFunctionCallOperationMetadata.NAME,
                "%11",
                ImmutableList.of(),
                ImmutableList.of(
                        singleBlockRegion(argumentsBlock),
                        singleBlockRegion(orderingSelectorBlock),
                        singleBlockRegion(frameStartFieldSelectorBlock),
                        singleBlockRegion(sortKeyCoercedForFrameStartComparisonSelectorBlock),
                        singleBlockRegion(frameEndFieldSelectorBlock),
                        singleBlockRegion(sortKeyCoercedForFrameEndComparisonSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "window_function_call:resolved_function"), lagFunction,
                        new AttributeKey(TRINO, "window_function_call:sort_orders"), new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "window_function_call:frame_type"), RANGE,
                        new AttributeKey(TRINO, "window_function_call:frame_start_type"), PRECEDING,
                        new AttributeKey(TRINO, "window_function_call:frame_end_type"), FOLLOWING,
                        new AttributeKey(TRINO, "window_function_call:ignore_nulls"), true,
                        new AttributeKey(TRINO, "window_function_call:distinct"), false,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("WindowFunctionCall operation must have exactly one argument: the window");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                WindowFunctionCallOperationMetadata.NAME,
                "%11",
                ImmutableList.of(windowFunctionsParameter),
                ImmutableList.of(
                        singleBlockRegion(argumentsBlock),
                        singleBlockRegion(sortKeyCoercedForFrameEndComparisonSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "window_function_call:resolved_function"), lagFunction,
                        new AttributeKey(TRINO, "window_function_call:sort_orders"), new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "window_function_call:frame_type"), RANGE,
                        new AttributeKey(TRINO, "window_function_call:frame_start_type"), PRECEDING,
                        new AttributeKey(TRINO, "window_function_call:frame_end_type"), FOLLOWING,
                        new AttributeKey(TRINO, "window_function_call:ignore_nulls"), true,
                        new AttributeKey(TRINO, "window_function_call:distinct"), false,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("WindowFunctionCall operation must have exactly six regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                WindowFunctionCallOperationMetadata.NAME,
                "%11",
                ImmutableList.of(windowFunctionsParameter),
                ImmutableList.of(
                        singleBlockRegion(argumentsBlock),
                        singleBlockRegion(orderingSelectorBlock),
                        singleBlockRegion(frameStartFieldSelectorBlock),
                        singleBlockRegion(sortKeyCoercedForFrameStartComparisonSelectorBlock),
                        singleBlockRegion(frameEndFieldSelectorBlock),
                        singleBlockRegion(sortKeyCoercedForFrameEndComparisonSelectorBlock)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "window_function_call:sort_orders"), new SortOrderList(ImmutableList.of(DESC_NULLS_LAST, ASC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "window_function_call:frame_type"), RANGE,
                        new AttributeKey(TRINO, "window_function_call:frame_start_type"), PRECEDING,
                        new AttributeKey(TRINO, "window_function_call:frame_end_type"), FOLLOWING,
                        new AttributeKey(TRINO, "window_function_call:ignore_nulls"), true,
                        new AttributeKey(TRINO, "window_function_call:distinct"), false,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("function is null");

        // collecting window functions in a row
        Row windowFunctionsRowOperation = new Row("%36", ImmutableList.of(windowFunctionCallOperation.result()), ImmutableList.of(windowFunctionCallOperation.attributes()));
        Return windowFunctionsReturnOperation = new Return("%37", windowFunctionsRowOperation.result(), windowFunctionsRowOperation.attributes());
        Block windowFunctionCallsBlock = new Block(
                Optional.of("^windowFunctions"),
                ImmutableList.of(windowFunctionsParameter),
                ImmutableList.of(
                        windowFunctionCallOperation,
                        windowFunctionsRowOperation,
                        windowFunctionsReturnOperation));

        // partitioning
        Parameter partitioningSelectorParameter = new Parameter(
                "%38",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationPartitioning = new FieldReference("%39", partitioningSelectorParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationPartitioning = new Row("%40", ImmutableList.of(fieldReferenceOperationPartitioning.result()), ImmutableList.of(fieldReferenceOperationPartitioning.attributes()));
        Return returnOperationPartitioning = new Return("%41", rowOperationPartitioning.result(), rowOperationPartitioning.attributes());
        Block partitioningSelectorBlock = new Block(
                Optional.of("^partitioningSelector"),
                ImmutableList.of(partitioningSelectorParameter),
                ImmutableList.of(
                        fieldReferenceOperationPartitioning,
                        rowOperationPartitioning,
                        returnOperationPartitioning));

        // ordering
        Parameter orderingParameter = new Parameter(
                "%42",
                VALUES_OPERATION_ROW_TYPE);
        FieldReference fieldReferenceOperationOrderingB = new FieldReference("%43", orderingParameter, 1, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        FieldReference fieldReferenceOperationOrderingA = new FieldReference("%44", orderingParameter, 0, DEFAULT_BLOCK_PARAMETER_ATTRIBUTES);
        Row rowOperationOrdering = new Row(
                "%45",
                ImmutableList.of(fieldReferenceOperationOrderingB.result(), fieldReferenceOperationOrderingA.result()),
                ImmutableList.of(fieldReferenceOperationOrderingB.attributes(), fieldReferenceOperationOrderingA.attributes()));
        Return returnOperationOrdering = new Return("%46", rowOperationOrdering.result(), rowOperationOrdering.attributes());
        Block orderingSelectorBlockWindow = new Block(
                Optional.of("^orderingSelector"),
                ImmutableList.of(orderingParameter),
                ImmutableList.of(
                        fieldReferenceOperationOrderingB,
                        fieldReferenceOperationOrderingA,
                        rowOperationOrdering,
                        returnOperationOrdering));

        Window windowOperation = new Window(
                "%9",
                VALUES_OPERATION.result(),
                windowFunctionCallsBlock,
                partitioningSelectorBlock,
                orderingSelectorBlockWindow,
                ImmutableList.of(0),
                Optional.of(new SortOrderList(ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_FIRST))),
                1,
                VALUES_OPERATION.attributes());

        Operation actualWindoeOperation = TESTING_TRINO_DIALECT.createOperation(
                WindowOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(windowFunctionCallsBlock),
                        singleBlockRegion(partitioningSelectorBlock),
                        singleBlockRegion(orderingSelectorBlockWindow)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "window:pre_partitioned_indexes"), ImmutableList.of(0),
                        new AttributeKey(TRINO, "window:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "window:pre_sorted_prefix"), 1,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false));

        assertThat(actualWindoeOperation).isEqualTo(windowOperation);
        assertThat(actualWindoeOperation.result().type()).isEqualTo(irType(new MultisetType(anonymousRow(BIGINT, BOOLEAN, BOOLEAN))));

        // wrong argument count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                WindowOperationMetadata.NAME,
                "%9",
                ImmutableList.of(),
                ImmutableList.of(
                        singleBlockRegion(windowFunctionCallsBlock),
                        singleBlockRegion(partitioningSelectorBlock),
                        singleBlockRegion(orderingSelectorBlockWindow)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "window:pre_partitioned_indexes"), ImmutableList.of(0),
                        new AttributeKey(TRINO, "window:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "window:pre_sorted_prefix"), 1,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Window operation must have exactly one argument: the input relation");

        // wrong region count
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                WindowOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(windowFunctionCallsBlock),
                        singleBlockRegion(orderingSelectorBlockWindow)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "window:pre_partitioned_indexes"), ImmutableList.of(0),
                        new AttributeKey(TRINO, "window:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_FIRST)),
                        new AttributeKey(TRINO, "window:pre_sorted_prefix"), 1,
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessage("Window operation must have exactly three regions");

        // missing required attribute
        assertThatThrownBy(() -> TESTING_TRINO_DIALECT.createOperation(
                WindowOperationMetadata.NAME,
                "%9",
                ImmutableList.of(VALUES_OPERATION.result()),
                ImmutableList.of(
                        singleBlockRegion(windowFunctionCallsBlock),
                        singleBlockRegion(partitioningSelectorBlock),
                        singleBlockRegion(orderingSelectorBlockWindow)),
                ImmutableMap.of(
                        new AttributeKey(TRINO, "window:pre_partitioned_indexes"), ImmutableList.of(0),
                        new AttributeKey(TRINO, "window:sort_orders"), new SortOrderList(ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_FIRST)),
                        // the IR level attributes must be enforced as they cannot be derived from source attributes, which are unavailable
                        new AttributeKey(IR, "repeatability"), DETERMINISTIC,
                        new AttributeKey(IR, "has_side_effects"), false)))
                .hasMessageMatching(".*the return value of .* is null");
    }

    private static Values valuesOperation()
    {
        Constant constantOperation1Row1 = new Constant("%1", BIGINT, 1L);
        Constant constantOperation2Row1 = new Constant("%2", BOOLEAN, true);
        Row rowOperation1 = new Row(
                "%3",
                ImmutableList.of(constantOperation1Row1.result(), constantOperation2Row1.result()),
                ImmutableList.of(constantOperation1Row1.attributes(), constantOperation2Row1.attributes()));
        Return returnOperation1 = new Return("%4", rowOperation1.result(), rowOperation1.attributes());
        Constant constantOperation1Row2 = new Constant("%5", BIGINT, 2L);
        Constant constantOperation2Row2 = new Constant("%6", BOOLEAN, false);
        Row rowOperation2 = new Row(
                "%7",
                ImmutableList.of(constantOperation1Row2.result(), constantOperation2Row2.result()),
                ImmutableList.of(constantOperation1Row2.attributes(), constantOperation2Row2.attributes()));
        Return returnOperation2 = new Return("%8", rowOperation2.result(), rowOperation2.attributes());

        return new Values(
                "%0",
                RowType.anonymous(ImmutableList.of(BIGINT, BOOLEAN)),
                ImmutableList.of(
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation1Row1,
                                        constantOperation2Row1,
                                        rowOperation1,
                                        returnOperation1)),
                        new Block(
                                Optional.of("^row"),
                                ImmutableList.of(),
                                ImmutableList.of(
                                        constantOperation1Row2,
                                        constantOperation2Row2,
                                        rowOperation2,
                                        returnOperation2))));
    }
}
