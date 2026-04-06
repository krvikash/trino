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

import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.cache.NonEvictableCache;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.aggregation.AccumulatorFactory;
import io.trino.spi.connector.SortOrder;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RowType.anonymousRow;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;

class TestPushAggregationIntoValues
        extends BaseRuleTest
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();
    private static final NonEvictableCache<PushAggregationIntoValues.AccumulatorFactoryKey, AccumulatorFactory> ACCUMULATOR_FACTORY_CACHE = buildNonEvictableCache(CacheBuilder.newBuilder());

    @Test
    void testCountStar()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(p.symbol("x", BIGINT)),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 1L)),
                                            ImmutableList.of(new Constant(BIGINT, 2L)),
                                            ImmutableList.of(new Constant(BIGINT, 3L)))))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .matches(values(
                        ImmutableList.of("count"),
                        ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 3L)))));
    }

    @Test
    void testSum()
    {
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol sum = p.symbol("sum", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 1L)),
                                            ImmutableList.of(new Constant(BIGINT, 2L)),
                                            ImmutableList.of(new Constant(BIGINT, 3L)))))
                            .addAggregation(sum, new Aggregation(
                                    sumFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .matches(values(
                        ImmutableList.of("sum"),
                        ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 6L)))));
    }

    @Test
    void testCountColumnSkipsNulls()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 1L)),
                                            ImmutableList.of(new Constant(BIGINT, null)),
                                            ImmutableList.of(new Constant(BIGINT, 3L)))))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .matches(values(
                        ImmutableList.of("count"),
                        ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 2L)))));
    }

    @Test
    void testMultipleAggregations()
    {
        ResolvedFunction countStarFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));
        ResolvedFunction minFunction = FUNCTION_RESOLUTION.resolveFunction("min", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol count = p.symbol("count", BIGINT);
                    Symbol sum = p.symbol("sum", BIGINT);
                    Symbol min = p.symbol("min", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 10L)),
                                            ImmutableList.of(new Constant(BIGINT, 20L)),
                                            ImmutableList.of(new Constant(BIGINT, 30L)))))
                            .addAggregation(count, new Aggregation(
                                    countStarFunction,
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .addAggregation(sum, new Aggregation(
                                    sumFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .addAggregation(min, new Aggregation(
                                    minFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .matches(values(
                        ImmutableList.of("count", "sum", "min"),
                        ImmutableList.of(ImmutableList.of(
                                new Constant(BIGINT, 3L),
                                new Constant(BIGINT, 60L),
                                new Constant(BIGINT, 10L)))));
    }

    @Test
    void testEmptyValues()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of()))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .matches(values(
                        ImmutableList.of("count"),
                        ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 0L)))));
    }

    @Test
    void testSumOverEmptyValues()
    {
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol sum = p.symbol("sum", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of()))
                            .addAggregation(sum, new Aggregation(
                                    sumFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .matches(values(
                        ImmutableList.of("sum"),
                        ImmutableList.of(ImmutableList.of(new Constant(BIGINT, null)))));
    }

    @Test
    void testAggregationWithMask()
    {
        ResolvedFunction avgFunction = FUNCTION_RESOLUTION.resolveFunction("avg", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol mask = p.symbol("mask", BOOLEAN);
                    Symbol avg = p.symbol("avg", DOUBLE);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x, mask),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 10L), new Constant(BOOLEAN, true)),
                                            ImmutableList.of(new Constant(BIGINT, 20L), new Constant(BOOLEAN, false)),
                                            ImmutableList.of(new Constant(BIGINT, 30L), new Constant(BOOLEAN, true)))))
                            .addAggregation(avg, new Aggregation(
                                    avgFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.of(mask)))
                            .globalGrouping());
                })
                .matches(values(
                        ImmutableList.of("avg"),
                        ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, 20.0)))));
    }

    @Test
    void testAggregationWithNullMask()
    {
        // Reproduces the Q17 decorrelation pattern: avg(null) mask(null)
        ResolvedFunction avgFunction = FUNCTION_RESOLUTION.resolveFunction("avg", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol mask = p.symbol("mask", BOOLEAN);
                    Symbol avg = p.symbol("avg", DOUBLE);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x, mask),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, null), new Constant(BOOLEAN, null)))))
                            .addAggregation(avg, new Aggregation(
                                    avgFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.of(mask)))
                            .globalGrouping());
                })
                .matches(values(
                        ImmutableList.of("avg"),
                        ImmutableList.of(ImmutableList.of(new Constant(DOUBLE, null)))));
    }

    @Test
    void testDoesNotFireForGroupedAggregation()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 1L)),
                                            ImmutableList.of(new Constant(BIGINT, 2L)))))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .singleGroupingSet(x));
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForDistinct()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 1L)),
                                            ImmutableList.of(new Constant(BIGINT, 1L)))))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    true,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForFilter()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol mask = p.symbol("mask", BIGINT);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x, mask),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BIGINT, 1L)))))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(),
                                    false,
                                    Optional.of(mask),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForNonSingleStep()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(p.symbol("x", BIGINT)),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, 1L)))))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .step(AggregationNode.Step.PARTIAL)
                            .globalGrouping());
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForNonDeterministicValues()
    {
        ResolvedFunction random = FUNCTION_RESOLUTION.resolveFunction("random", fromTypes());
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(DOUBLE));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", DOUBLE);
                    Symbol sum = p.symbol("sum", DOUBLE);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(ImmutableList.of(new Call(random, ImmutableList.of())))))
                            .addAggregation(sum, new Aggregation(
                                    sumFunction,
                                    ImmutableList.of(new Reference(DOUBLE, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForOrderBy()
    {
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol sum = p.symbol("sum", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 1L)))))
                            .addAggregation(sum, new Aggregation(
                                    sumFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.of(new OrderingScheme(ImmutableList.of(x), ImmutableMap.of(x, SortOrder.ASC_NULLS_FIRST))),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForMaskNotInValuesOutputs()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol externalMask = p.symbol("external_mask", BOOLEAN);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(ImmutableList.of(new Constant(BIGINT, 1L)))))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.of(externalMask)))
                            .globalGrouping());
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForCorrelatedValues()
    {
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol sum = p.symbol("sum", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(ImmutableList.of(new Reference(BIGINT, "correlated")))))
                            .addAggregation(sum, new Aggregation(
                                    sumFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForValuesWithNoOutputs()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(3))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .doesNotFire();
    }

    @Test
    void testDoesNotFireForNonRowExpressions()
    {
        ResolvedFunction countFunction = FUNCTION_RESOLUTION.resolveFunction("count", fromTypes());
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol a = p.symbol("a", BIGINT);
                    Symbol count = p.symbol("count", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.valuesOfExpressions(
                                    ImmutableList.of(a),
                                    ImmutableList.of(
                                            new Cast(new Row(ImmutableList.of(new Constant(INTEGER, 1L))), anonymousRow(BIGINT)))))
                            .addAggregation(count, new Aggregation(
                                    countFunction,
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .doesNotFire();
    }

    @Test
    void testSumOverAllNulls()
    {
        ResolvedFunction sumFunction = FUNCTION_RESOLUTION.resolveFunction("sum", fromTypes(BIGINT));
        tester().assertThat(new PushAggregationIntoValues(tester().getPlannerContext(), ACCUMULATOR_FACTORY_CACHE))
                .on(p -> {
                    Symbol x = p.symbol("x", BIGINT);
                    Symbol sum = p.symbol("sum", BIGINT);
                    return p.aggregation(builder -> builder
                            .source(p.values(
                                    ImmutableList.of(x),
                                    ImmutableList.of(
                                            ImmutableList.of(new Constant(BIGINT, null)),
                                            ImmutableList.of(new Constant(BIGINT, null)))))
                            .addAggregation(sum, new Aggregation(
                                    sumFunction,
                                    ImmutableList.of(new Reference(BIGINT, "x")),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()))
                            .globalGrouping());
                })
                .matches(values(
                        ImmutableList.of("sum"),
                        ImmutableList.of(ImmutableList.of(new Constant(BIGINT, null)))));
    }
}
