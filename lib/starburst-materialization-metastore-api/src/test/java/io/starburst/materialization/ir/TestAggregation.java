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
package io.starburst.materialization.ir;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.starburst.materialization.ir.Aggregation.AggregateFunctionCall;
import io.starburst.materialization.ir.Aggregation.GroupingSets;
import io.starburst.materialization.ir.Aggregation.OrderingScheme;
import io.starburst.materialization.ir.Aggregation.SortItem;
import io.trino.spi.function.BoundSignature;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.materialization.ir.Aggregation.Step.FINAL;
import static io.starburst.materialization.ir.Aggregation.Step.PARTIAL;
import static io.starburst.materialization.ir.Aggregation.Step.SINGLE;
import static io.starburst.materialization.ir.IrFixtures.builtinSignature;
import static io.starburst.materialization.ir.IrFixtures.tableScan;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.connector.SortOrder.DESC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestAggregation
{
    private static final Symbol NAME = new Symbol(VARCHAR, "name");
    private static final Symbol VALUE = new Symbol(BIGINT, "value");
    private static final Symbol FLAG = new Symbol(BOOLEAN, "flag");
    private static final TableScan SCAN = tableScan(NAME, VALUE, FLAG);
    private static final Symbol TOTAL = new Symbol(BIGINT, "total");
    private static final BoundSignature SUM = builtinSignature("sum", BIGINT, BIGINT);

    @Test
    void testOutputsAreGroupingKeysFollowedByAggregationOutputs()
    {
        Aggregation aggregate = aggregate(SCAN, ImmutableList.of(NAME), sum(TOTAL, VALUE));

        assertThat(aggregate.outputs()).containsExactly(NAME, TOTAL);
    }

    @Test
    void testAcceptsFilterMaskAndOrdering()
    {
        AggregateFunctionCall aggregation = new AggregateFunctionCall(
                TOTAL,
                SUM,
                ImmutableList.of(new Reference(VALUE)),
                true,
                Optional.of(FLAG),
                Optional.of(new OrderingScheme(ImmutableList.of(new SortItem(NAME, ASC_NULLS_LAST), new SortItem(VALUE, DESC_NULLS_FIRST)))),
                Optional.of(FLAG));

        assertThat(aggregate(SCAN, ImmutableList.of(NAME), aggregation).aggregations()).containsExactly(aggregation);
    }

    @Test
    void testAcceptsNestedAggregate()
    {
        Aggregation inner = aggregate(SCAN, ImmutableList.of(NAME, FLAG), sum(TOTAL, VALUE));
        Symbol outerTotal = new Symbol(BIGINT, "outer_total");

        assertThat(aggregate(inner, ImmutableList.of(NAME), sum(outerTotal, TOTAL)).outputs()).containsExactly(NAME, outerTotal);
    }

    @Test
    void testRejectsGroupingKeyNotProducedBySource()
    {
        assertThatThrownBy(() -> aggregate(SCAN, ImmutableList.of(new Symbol(VARCHAR, "missing")), sum(TOTAL, VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: grouping key is not an output of the source");
    }

    @Test
    void testRejectsArgumentNotProducedBySource()
    {
        assertThatThrownBy(() -> aggregate(SCAN, ImmutableList.of(NAME), sum(TOTAL, new Symbol(BIGINT, "missing"))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: argument of function system.builtin.sum is not an output of the source");
    }

    @Test
    void testRejectsNonReferenceArgument()
    {
        assertThatThrownBy(() -> new AggregateFunctionCall(
                TOTAL,
                builtinSignature("count", BIGINT, BOOLEAN),
                ImmutableList.of(new IsNull(new Reference(VALUE))),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: argument of function system.builtin.count is not a reference");
    }

    @Test
    void testRejectsArgumentCountNotMatchingFunction()
    {
        assertThatThrownBy(() -> aggregate(SCAN, ImmutableList.of(NAME), aggregation(TOTAL, SUM, VALUE, VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: SINGLE function system.builtin.sum expects 1 arguments, got 2");
    }

    @Test
    void testFinalStepTakesIntermediateState()
    {
        Aggregation aggregate = new Aggregation(SCAN, singleGroupingSet(ImmutableList.of(NAME)), ImmutableList.of(aggregation(TOTAL, builtinSignature("count", BIGINT), VALUE)), FINAL, Optional.empty());

        assertThat(aggregate.outputs()).containsExactly(NAME, TOTAL);
    }

    @Test
    void testRejectsOrderingOutsideSingleStep()
    {
        OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(new SortItem(VALUE, ASC_NULLS_LAST)));
        AggregateFunctionCall aggregation = new AggregateFunctionCall(TOTAL, SUM, ImmutableList.of(new Reference(VALUE)), false, Optional.empty(), Optional.of(orderingScheme), Optional.empty());

        assertThatThrownBy(() -> new Aggregation(SCAN, singleGroupingSet(ImmutableList.of(NAME)), ImmutableList.of(aggregation), PARTIAL, Optional.empty()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: ORDER BY requires step SINGLE");
    }

    @Test
    void testRejectsEmptyOrdering()
    {
        assertThatThrownBy(() -> new OrderingScheme(ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: ordering has no sort items");
    }

    @Test
    void testRejectsOrderingBySymbolTwice()
    {
        assertThatThrownBy(() -> new OrderingScheme(ImmutableList.of(new SortItem(VALUE, ASC_NULLS_LAST), new SortItem(VALUE, DESC_NULLS_FIRST))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: ordering sorts by a symbol more than once");
    }

    @Test
    void testRejectsNonPositiveGroupingSetCount()
    {
        assertThatThrownBy(() -> new GroupingSets(ImmutableList.of(NAME), 0, ImmutableSet.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: grouping set count must be positive");
    }

    @Test
    void testRejectsMoreGlobalGroupingSetsThanGroupingSets()
    {
        assertThatThrownBy(() -> new GroupingSets(ImmutableList.of(NAME), 1, ImmutableSet.of(0, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: more global grouping sets than grouping sets");
    }

    @Test
    void testRejectsNoGroupingKeysWithoutGlobalGroupingSet()
    {
        assertThatThrownBy(() -> new GroupingSets(ImmutableList.of(), 1, ImmutableSet.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: no grouping keys requires a global grouping set");
    }

    @Test
    void testRejectsFilterNotProducedBySource()
    {
        AggregateFunctionCall aggregation = new AggregateFunctionCall(TOTAL, SUM, ImmutableList.of(new Reference(VALUE)), false, Optional.of(new Symbol(FLAG.type(), "missing")), Optional.empty(), Optional.empty());

        assertThatThrownBy(() -> aggregate(SCAN, ImmutableList.of(NAME), aggregation))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: filter of function system.builtin.sum is not an output of the source");
    }

    @Test
    void testRejectsMaskNotProducedBySource()
    {
        AggregateFunctionCall aggregation = new AggregateFunctionCall(TOTAL, SUM, ImmutableList.of(new Reference(VALUE)), false, Optional.empty(), Optional.empty(), Optional.of(new Symbol(FLAG.type(), "missing")));

        assertThatThrownBy(() -> aggregate(SCAN, ImmutableList.of(NAME), aggregation))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: mask of function system.builtin.sum is not an output of the source");
    }

    @Test
    void testRejectsSortItemNotProducedBySource()
    {
        OrderingScheme orderingScheme = new OrderingScheme(ImmutableList.of(new SortItem(new Symbol(BIGINT, "missing"), ASC_NULLS_LAST)));
        AggregateFunctionCall aggregation = new AggregateFunctionCall(TOTAL, SUM, ImmutableList.of(new Reference(VALUE)), false, Optional.empty(), Optional.of(orderingScheme), Optional.empty());

        assertThatThrownBy(() -> aggregate(SCAN, ImmutableList.of(NAME), aggregation))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: sort item of function system.builtin.sum is not an output of the source");
    }

    @Test
    void testRejectsAggregationOutputBoundInSource()
    {
        assertThatThrownBy(() -> aggregate(SCAN, ImmutableList.of(NAME), sum(new Symbol(BIGINT, VALUE.name()), VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: aggregation output name is already bound in the source");
    }

    @Test
    void testRejectsAggregationOutputBoundDeeperInSource()
    {
        // value is bound by the scan but not visible above the inner aggregate, which drops it
        Aggregation inner = aggregate(SCAN, ImmutableList.of(NAME), sum(TOTAL, VALUE));

        assertThatThrownBy(() -> aggregate(inner, ImmutableList.of(NAME), sum(new Symbol(BIGINT, VALUE.name()), TOTAL)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: aggregation output name is already bound in the source");
    }

    @Test
    void testRejectsDuplicateAggregationOutputs()
    {
        assertThatThrownBy(() -> aggregate(SCAN, ImmutableList.of(NAME), sum(TOTAL, VALUE), sum(TOTAL, VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: aggregation output name is bound more than once");
    }

    @Test
    void testRejectsGroupIdSymbolNotAGroupingKey()
    {
        assertThatThrownBy(() -> new Aggregation(SCAN, singleGroupingSet(ImmutableList.of(NAME)), ImmutableList.of(sum(TOTAL, VALUE)), SINGLE, Optional.of(VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: group id symbol is not a grouping key");
    }

    @Test
    void testRejectsUnsupportedFunction()
    {
        BoundSignature approxDistinct = builtinSignature("approx_distinct", BIGINT, BIGINT);

        assertThatThrownBy(() -> aggregate(SCAN, ImmutableList.of(NAME), aggregation(TOTAL, approxDistinct, VALUE)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Aggregation: function system.builtin.approx_distinct is not supported");
    }

    private static Aggregation aggregate(Operation source, List<Symbol> groupingKeys, AggregateFunctionCall... aggregations)
    {
        return new Aggregation(source, singleGroupingSet(groupingKeys), ImmutableList.copyOf(aggregations), SINGLE, Optional.empty());
    }

    private static GroupingSets singleGroupingSet(List<Symbol> groupingKeys)
    {
        return new GroupingSets(groupingKeys, 1, groupingKeys.isEmpty() ? ImmutableSet.of(0) : ImmutableSet.of());
    }

    private static AggregateFunctionCall sum(Symbol output, Symbol argument)
    {
        return aggregation(output, SUM, argument);
    }

    private static AggregateFunctionCall aggregation(Symbol output, BoundSignature function, Symbol... arguments)
    {
        return new AggregateFunctionCall(
                output,
                function,
                Stream.of(arguments)
                        .<Expression>map(Reference::new)
                        .collect(toImmutableList()),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }
}
