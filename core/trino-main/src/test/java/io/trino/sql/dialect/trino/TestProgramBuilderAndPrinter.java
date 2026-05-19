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
package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.connector.CatalogHandle;
import io.trino.connector.TestingColumnHandle;
import io.trino.cost.PlanNodeStatsEstimate;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TableHandle;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Logical;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningHandle;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TestingConnectorTransactionHandle;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.GroupingSetDescriptor;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.CorrelatedJoinNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinType;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.tree.Identifier;
import io.trino.testing.TestingTransactionHandle;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.spi.connector.SortOrder.ASC_NULLS_LAST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Logical.Operator.AND;
import static io.trino.sql.newir.FormatOptions.TESTING_PRINT_OPTIONS;
import static org.assertj.core.api.Assertions.assertThat;

final class TestProgramBuilderAndPrinter
{
    @Test
    public void testSimplePlan()
    {
        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new FilterNode(
                                        new PlanNodeId("101"),
                                        new ValuesNode(
                                                new PlanNodeId("102"),
                                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                ImmutableList.of(
                                                        new Row(ImmutableList.of(new Constant(BIGINT, 3L), new Constant(BOOLEAN, true))),
                                                        new Row(ImmutableList.of(new Constant(BIGINT, 5L), new Constant(BOOLEAN, false))))),
                                        new Constant(BOOLEAN, true)),
                                ImmutableList.of("col_a"),
                                ImmutableList.of(new Symbol(BIGINT, "a"))))
                .print(TESTING_PRINT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = values() : () -> "multiset(row(bigint,boolean))" ({
                                    ^row
                                        %2 = constant() : () -> "bigint" ()
                                            {constant:value = "[type=bigint, value=3]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %3 = constant() : () -> "boolean" ()
                                            {constant:value = "[type=boolean, value=true]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %4 = row(%2, %3) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %5 = return(%4) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    }, {
                                    ^row
                                        %6 = constant() : () -> "bigint" ()
                                            {constant:value = "[type=bigint, value=5]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %7 = constant() : () -> "boolean" ()
                                            {constant:value = "[type=boolean, value=false]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %8 = row(%6, %7) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %9 = return(%8) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {values:cardinality = "2", values:row_type = "row(bigint,boolean)", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                %10 = filter(%1) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean))" ({
                                    ^predicate (%11 : "row(bigint,boolean)")
                                        %12 = constant() : () -> "boolean" ()
                                            {constant:value = "[type=boolean, value=true]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %13 = return(%12) : ("boolean") -> "boolean" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                %14 = output(%10) : ("multiset(row(bigint,boolean))") -> "boolean" ({
                                    ^outputFieldSelector (%15 : "row(bigint,boolean)")
                                        %16 = field_reference(%15) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %17 = row(%16) : ("bigint") -> "row(bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %18 = return(%17) : ("row(bigint)") -> "row(bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {output:column_names = "[""col_a""]", ir.has_side_effects = "true", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                            })
                            {ir.has_side_effects = "true", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                        """);
    }

    @Test
    public void testMultiLevelCorrelation()
    {
        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new CorrelatedJoinNode(
                                        new PlanNodeId("101"),
                                        new ValuesNode(
                                                new PlanNodeId("102"),
                                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                ImmutableList.of(
                                                        new Row(ImmutableList.of(new Constant(BIGINT, 1L), new Constant(BOOLEAN, true))),
                                                        new Row(ImmutableList.of(new Constant(BIGINT, 2L), new Constant(BOOLEAN, false))))),
                                        new CorrelatedJoinNode(
                                                new PlanNodeId("103"),
                                                new ValuesNode(
                                                        new PlanNodeId("104"),
                                                        ImmutableList.of(new Symbol(BIGINT, "c"), new Symbol(BOOLEAN, "d")),
                                                        ImmutableList.of(
                                                                new Row(ImmutableList.of(new Constant(BIGINT, 3L), new Constant(BOOLEAN, true))),
                                                                new Row(ImmutableList.of(new Constant(BIGINT, 4L), new Constant(BOOLEAN, false))))),
                                                new FilterNode(
                                                        new PlanNodeId("105"),
                                                        new ValuesNode(
                                                                new PlanNodeId("106"),
                                                                ImmutableList.of(new Symbol(BIGINT, "e"), new Symbol(BOOLEAN, "f")),
                                                                ImmutableList.of(
                                                                        new Row(ImmutableList.of(new Reference(BIGINT, "a"), new Reference(BOOLEAN, "b"))), // correlated level 1
                                                                        new Row(ImmutableList.of(new Reference(BIGINT, "c"), new Reference(BOOLEAN, "d"))))), // correlated level 2
                                                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "b"), new Reference(BOOLEAN, "d")))), // correlated on 2 levels
                                                ImmutableList.of(new Symbol(BIGINT, "c"), new Symbol(BOOLEAN, "d")),
                                                JoinType.INNER,
                                                new Reference(BOOLEAN, "b"),
                                                new Identifier("bla")), // origin subquery, whatever
                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                        JoinType.INNER,
                                        new Logical(AND, ImmutableList.of(new Reference(BOOLEAN, "b"), new Reference(BOOLEAN, "d"), new Reference(BOOLEAN, "f"))), // filter using input and subquery symbols
                                        new Identifier("bla")), // origin subquery, whatever
                                ImmutableList.of("col_1", "col_2", "col_3", "col_4", "col_5", "col_6"),
                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"), new Symbol(BIGINT, "c"), new Symbol(BOOLEAN, "d"), new Symbol(BIGINT, "e"), new Symbol(BOOLEAN, "f"))))
                .print(TESTING_PRINT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = values() : () -> "multiset(row(bigint,boolean))" ({
                                    ^row
                                        %2 = constant() : () -> "bigint" ()
                                            {constant:value = "[type=bigint, value=1]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %3 = constant() : () -> "boolean" ()
                                            {constant:value = "[type=boolean, value=true]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %4 = row(%2, %3) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %5 = return(%4) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    }, {
                                    ^row
                                        %6 = constant() : () -> "bigint" ()
                                            {constant:value = "[type=bigint, value=2]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %7 = constant() : () -> "boolean" ()
                                            {constant:value = "[type=boolean, value=false]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %8 = row(%6, %7) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %9 = return(%8) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {values:cardinality = "2", values:row_type = "row(bigint,boolean)", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                %10 = correlated_join(%1) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean,bigint,boolean,bigint,boolean))" ({
                                    ^correlationSelector (%11 : "row(bigint,boolean)")
                                        %12 = field_reference(%11) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %13 = field_reference(%11) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %14 = row(%12, %13) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %15 = return(%14) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    }, {
                                    ^subquery (%16 : "row(bigint,boolean)")
                                        %17 = values() : () -> "multiset(row(bigint,boolean))" ({
                                            ^row
                                                %18 = constant() : () -> "bigint" ()
                                                    {constant:value = "[type=bigint, value=3]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %19 = constant() : () -> "boolean" ()
                                                    {constant:value = "[type=boolean, value=true]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %20 = row(%18, %19) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %21 = return(%20) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            }, {
                                            ^row
                                                %22 = constant() : () -> "bigint" ()
                                                    {constant:value = "[type=bigint, value=4]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %23 = constant() : () -> "boolean" ()
                                                    {constant:value = "[type=boolean, value=false]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %24 = row(%22, %23) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %25 = return(%24) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            })
                                            {values:cardinality = "2", values:row_type = "row(bigint,boolean)", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %26 = correlated_join(%17) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean,bigint,boolean))" ({
                                            ^correlationSelector (%27 : "row(bigint,boolean)")
                                                %28 = field_reference(%27) : ("row(bigint,boolean)") -> "bigint" ()
                                                    {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %29 = field_reference(%27) : ("row(bigint,boolean)") -> "boolean" ()
                                                    {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %30 = row(%28, %29) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %31 = return(%30) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            }, {
                                            ^subquery (%32 : "row(bigint,boolean)")
                                                %33 = values() : () -> "multiset(row(bigint,boolean))" ({
                                                    ^row
                                                        %34 = field_reference(%16) : ("row(bigint,boolean)") -> "bigint" ()
                                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                        %35 = field_reference(%16) : ("row(bigint,boolean)") -> "boolean" ()
                                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                        %36 = row(%34, %35) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                        %37 = return(%36) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                                    }, {
                                                    ^row
                                                        %38 = field_reference(%32) : ("row(bigint,boolean)") -> "bigint" ()
                                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                        %39 = field_reference(%32) : ("row(bigint,boolean)") -> "boolean" ()
                                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                        %40 = row(%38, %39) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                        %41 = return(%40) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                                    })
                                                    {values:cardinality = "2", values:row_type = "row(bigint,boolean)", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %42 = filter(%33) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean))" ({
                                                    ^predicate (%43 : "row(bigint,boolean)")
                                                        %44 = field_reference(%16) : ("row(bigint,boolean)") -> "boolean" ()
                                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                        %45 = field_reference(%32) : ("row(bigint,boolean)") -> "boolean" ()
                                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                        %46 = logical(%44, %45) : ("boolean", "boolean") -> "boolean" ()
                                                            {logical:operator = "AND", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                        %47 = return(%46) : ("boolean") -> "boolean" ()
                                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                                    })
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %48 = return(%42) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean))" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            }, {
                                            ^filter (%49 : "row(bigint,boolean)", %50 : "row(bigint,boolean)")
                                                %51 = field_reference(%16) : ("row(bigint,boolean)") -> "boolean" ()
                                                    {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %52 = return(%51) : ("boolean") -> "boolean" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            })
                                            {correlated_join:type = "INNER", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %53 = return(%26) : ("multiset(row(bigint,boolean,bigint,boolean))") -> "multiset(row(bigint,boolean,bigint,boolean))" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    }, {
                                    ^filter (%54 : "row(bigint,boolean)", %55 : "row(bigint,boolean,bigint,boolean)")
                                        %56 = field_reference(%54) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %57 = field_reference(%55) : ("row(bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %58 = field_reference(%55) : ("row(bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "3", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %59 = logical(%56, %57, %58) : ("boolean", "boolean", "boolean") -> "boolean" ()
                                            {logical:operator = "AND", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %60 = return(%59) : ("boolean") -> "boolean" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {correlated_join:type = "INNER", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                %61 = output(%10) : ("multiset(row(bigint,boolean,bigint,boolean,bigint,boolean))") -> "boolean" ({
                                    ^outputFieldSelector (%62 : "row(bigint,boolean,bigint,boolean,bigint,boolean)")
                                        %63 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %64 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %65 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "2", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %66 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "3", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %67 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "4", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %68 = field_reference(%62) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "5", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %69 = row(%63, %64, %65, %66, %67, %68) : ("bigint", "boolean", "bigint", "boolean", "bigint", "boolean") -> "row(bigint,boolean,bigint,boolean,bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %70 = return(%69) : ("row(bigint,boolean,bigint,boolean,bigint,boolean)") -> "row(bigint,boolean,bigint,boolean,bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {output:column_names = "[""col_1"",""col_2"",""col_3"",""col_4"",""col_5"",""col_6""]", ir.has_side_effects = "true", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                            })
                            {ir.has_side_effects = "true", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                        """);
    }

    @Test
    public void testProjectAndAggregation()
    {
        TestingFunctionResolution functionResolution = new TestingFunctionResolution();
        ResolvedFunction countFunction = functionResolution.resolveFunction("count", ImmutableList.of());
        ResolvedFunction sumFunction = functionResolution.resolveFunction("sum", fromTypes(BIGINT));

        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new AggregationNode(
                                        new PlanNodeId("101"),
                                        new ProjectNode(
                                                new PlanNodeId("102"),
                                                new ValuesNode(
                                                        new PlanNodeId("103"),
                                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                        ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 3L), new Constant(BOOLEAN, true))))),
                                                Assignments.builder()
                                                        .putIdentities(ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")))
                                                        .put(new Symbol(BIGINT, "c"), new Constant(BIGINT, 5L))
                                                        .build()),
                                        ImmutableMap.of(
                                                new Symbol(BIGINT, "count"), new AggregationNode.Aggregation(
                                                        countFunction,
                                                        ImmutableList.of(),
                                                        true,
                                                        Optional.of(new Symbol(BOOLEAN, "b")),
                                                        Optional.empty(),
                                                        Optional.empty()),
                                                new Symbol(BIGINT, "sum"), new AggregationNode.Aggregation(
                                                        sumFunction,
                                                        ImmutableList.of(new Reference(BIGINT, "c")),
                                                        false,
                                                        Optional.empty(),
                                                        Optional.of(new OrderingScheme(ImmutableList.of(new Symbol(BIGINT, "a")), ImmutableMap.of(new Symbol(BIGINT, "a"), ASC_NULLS_LAST))),
                                                        Optional.of(new Symbol(BOOLEAN, "b")))),
                                        new GroupingSetDescriptor(ImmutableList.of(new Symbol(BIGINT, "c")), 2, ImmutableSet.of(1)),
                                        ImmutableList.of(new Symbol(BIGINT, "c")),
                                        AggregationNode.Step.SINGLE,
                                        Optional.empty(),
                                        Optional.empty()),
                                ImmutableList.of("key_c", "count", "sum"),
                                ImmutableList.of(new Symbol(BIGINT, "c"), new Symbol(BIGINT, "count"), new Symbol(BIGINT, "sum"))))
                .print(TESTING_PRINT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = values() : () -> "multiset(row(bigint,boolean))" ({
                                    ^row
                                        %2 = constant() : () -> "bigint" ()
                                            {constant:value = "[type=bigint, value=3]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %3 = constant() : () -> "boolean" ()
                                            {constant:value = "[type=boolean, value=true]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %4 = row(%2, %3) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %5 = return(%4) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {values:cardinality = "1", values:row_type = "row(bigint,boolean)", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                %6 = project(%1) : ("multiset(row(bigint,boolean))") -> "multiset(row(bigint,boolean,bigint))" ({
                                    ^assignments (%7 : "row(bigint,boolean)")
                                        %8 = field_reference(%7) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %9 = field_reference(%7) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %10 = constant() : () -> "bigint" ()
                                            {constant:value = "[type=bigint, value=5]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %11 = row(%8, %9, %10) : ("bigint", "boolean", "bigint") -> "row(bigint,boolean,bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %12 = return(%11) : ("row(bigint,boolean,bigint)") -> "row(bigint,boolean,bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                %13 = aggregation(%6) : ("multiset(row(bigint,boolean,bigint))") -> "multiset(row(bigint,bigint,bigint))" ({
                                    ^aggregates (%14 : "multiset(row(bigint,boolean,bigint))")
                                        %15 = aggregate_call(%14) : ("multiset(row(bigint,boolean,bigint))") -> "bigint" ({
                                            ^arguments (%16 : "row(bigint,boolean,bigint)")
                                                %17 = constant() : () -> "empty row" ()
                                                    {constant:value = "[type=empty row, value=null]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %18 = return(%17) : ("empty row") -> "empty row" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            }, {
                                            ^filterSelector (%19 : "row(bigint,boolean,bigint)")
                                                %20 = field_reference(%19) : ("row(bigint,boolean,bigint)") -> "boolean" ()
                                                    {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %21 = row(%20) : ("boolean") -> "row(boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %22 = return(%21) : ("row(boolean)") -> "row(boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            }, {
                                            ^maskSelector (%23 : "row(bigint,boolean,bigint)")
                                                %24 = constant() : () -> "empty row" ()
                                                    {constant:value = "[type=empty row, value=null]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %25 = return(%24) : ("empty row") -> "empty row" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            }, {
                                            ^orderingSelector (%26 : "row(bigint,boolean,bigint)")
                                                %27 = constant() : () -> "empty row" ()
                                                    {constant:value = "[type=empty row, value=null]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %28 = return(%27) : ("empty row") -> "empty row" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            })
                                            {aggregate_call:resolved_function = "{""signature"":{""name"":{""catalogName"":""system"",""schemaName"":""builtin"",""functionName"":""count""},""returnType"":""bigint"",""argumentTypes"":[]},""catalogHandle"":""system:normal:system"",""functionId"":""count():bigint"",""functionKind"":""AGGREGATE"",""deterministic"":true,""neverFails"":false,""functionNullability"":{""returnNullable"":true,""argumentNullable"":[]},""typeDependencies"":{},""functionDependencies"":[]}", aggregate_call:distinct = "true", aggregate_call:step = "SINGLE", aggregate_call:result_type = "bigint", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC"}
                                        %29 = aggregate_call(%14) : ("multiset(row(bigint,boolean,bigint))") -> "bigint" ({
                                            ^arguments (%30 : "row(bigint,boolean,bigint)")
                                                %31 = field_reference(%30) : ("row(bigint,boolean,bigint)") -> "bigint" ()
                                                    {field_reference:index = "2", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %32 = row(%31) : ("bigint") -> "row(bigint)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %33 = return(%32) : ("row(bigint)") -> "row(bigint)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            }, {
                                            ^filterSelector (%34 : "row(bigint,boolean,bigint)")
                                                %35 = constant() : () -> "empty row" ()
                                                    {constant:value = "[type=empty row, value=null]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %36 = return(%35) : ("empty row") -> "empty row" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            }, {
                                            ^maskSelector (%37 : "row(bigint,boolean,bigint)")
                                                %38 = field_reference(%37) : ("row(bigint,boolean,bigint)") -> "boolean" ()
                                                    {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %39 = row(%38) : ("boolean") -> "row(boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %40 = return(%39) : ("row(boolean)") -> "row(boolean)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            }, {
                                            ^orderingSelector (%41 : "row(bigint,boolean,bigint)")
                                                %42 = field_reference(%41) : ("row(bigint,boolean,bigint)") -> "bigint" ()
                                                    {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %43 = row(%42) : ("bigint") -> "row(bigint)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                                %44 = return(%43) : ("row(bigint)") -> "row(bigint)" ()
                                                    {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                            })
                                            {aggregate_call:sort_orders = "[""ASC_NULLS_LAST""]", aggregate_call:resolved_function = "{""signature"":{""name"":{""catalogName"":""system"",""schemaName"":""builtin"",""functionName"":""sum""},""returnType"":""bigint"",""argumentTypes"":[""bigint""]},""catalogHandle"":""system:normal:system"",""functionId"":""sum(bigint):bigint"",""functionKind"":""AGGREGATE"",""deterministic"":true,""neverFails"":false,""functionNullability"":{""returnNullable"":true,""argumentNullable"":[false]},""typeDependencies"":{},""functionDependencies"":[]}", aggregate_call:distinct = "false", aggregate_call:step = "SINGLE", aggregate_call:result_type = "bigint", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC"}
                                        %45 = row(%15, %29) : ("bigint", "bigint") -> "row(bigint,bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC"}
                                        %46 = return(%45) : ("row(bigint,bigint)") -> "row(bigint,bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.terminal = "true"}
                                    }, {
                                    ^groupingKeysSelector (%47 : "row(bigint,boolean,bigint)")
                                        %48 = field_reference(%47) : ("row(bigint,boolean,bigint)") -> "bigint" ()
                                            {field_reference:index = "2", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %49 = row(%48) : ("bigint") -> "row(bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %50 = return(%49) : ("row(bigint)") -> "row(bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {aggregation:grouping_sets_count = "2", aggregation:global_grouping_sets = "[1]", aggregation:pre_grouped_indexes = "[0]", aggregation:step = "SINGLE", aggregation:input_reducing = "false", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC"}
                                %51 = output(%13) : ("multiset(row(bigint,bigint,bigint))") -> "boolean" ({
                                    ^outputFieldSelector (%52 : "row(bigint,bigint,bigint)")
                                        %53 = field_reference(%52) : ("row(bigint,bigint,bigint)") -> "bigint" ()
                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %54 = field_reference(%52) : ("row(bigint,bigint,bigint)") -> "bigint" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %55 = field_reference(%52) : ("row(bigint,bigint,bigint)") -> "bigint" ()
                                            {field_reference:index = "2", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %56 = row(%53, %54, %55) : ("bigint", "bigint", "bigint") -> "row(bigint,bigint,bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %57 = return(%56) : ("row(bigint,bigint,bigint)") -> "row(bigint,bigint,bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {output:column_names = "[""key_c"",""count"",""sum""]", ir.has_side_effects = "true", ir.repeatability = "DETERMINISTIC", ir.terminal = "true"}
                            })
                            {ir.has_side_effects = "true", ir.repeatability = "DETERMINISTIC", ir.terminal = "true"}
                        """);
    }

    @Test
    public void testTableScan()
    {
        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new TableScanNode(
                                        new PlanNodeId("101"),
                                        new TableHandle(
                                                CatalogHandle.fromId("bla:normal:1"),
                                                TestingConnectorTableHandle.INSTANCE,
                                                TestingTransactionHandle.create()),
                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                        ImmutableMap.of(
                                                new Symbol(BOOLEAN, "b"), new TestingColumnHandle("b_handle"),
                                                new Symbol(BIGINT, "a"), new TestingColumnHandle("a_handle")),
                                        TupleDomain.withColumnDomains(ImmutableMap.of(new TestingColumnHandle("b_handle"), Domain.singleValue(BOOLEAN, true))),
                                        Optional.of(PlanNodeStatsEstimate.unknown()),
                                        false,
                                        Optional.of(Boolean.TRUE)),
                                ImmutableList.of("col_a", "col_b"),
                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b"))))
                .print(TESTING_PRINT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = table_scan() : () -> "multiset(row(bigint,boolean))" ()
                                    {table_scan:table_handle = "bla:INSTANCE", table_scan:column_handles = "TestingColumnHandle{name=a_handle}, TestingColumnHandle{name=b_handle}", table_scan:constraint = "{TestingColumnHandle{name=b_handle}=[ SortedRangeSet[type=boolean, ranges=1, {[true]}] ]}", table_scan:statistics = "{""outputRowCount"":""NaN"",""fieldStatistics"":{}}", table_scan:update_target = "false", table_scan:use_connector_node_partitioning = "true", table_scan:row_type = "row(bigint,boolean)", ir.has_side_effects = "false", ir.safe = "true"}
                                %2 = output(%1) : ("multiset(row(bigint,boolean))") -> "boolean" ({
                                    ^outputFieldSelector (%3 : "row(bigint,boolean)")
                                        %4 = field_reference(%3) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %5 = field_reference(%3) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %6 = row(%4, %5) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %7 = return(%6) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {output:column_names = "[""col_a"",""col_b""]", ir.has_side_effects = "true", ir.safe = "true", ir.terminal = "true"}
                            })
                            {ir.has_side_effects = "true", ir.safe = "true", ir.terminal = "true"}
                        """);
    }

    @Test
    public void testExchange()
    {
        assertThat(ProgramBuilder.buildProgram(
                        new OutputNode(
                                new PlanNodeId("100"),
                                new ExchangeNode(
                                        new PlanNodeId("101"),
                                        ExchangeNode.Type.GATHER,
                                        ExchangeNode.Scope.REMOTE,
                                        new PartitioningScheme(
                                                Partitioning.create(
                                                        new PartitioningHandle(
                                                                Optional.of(CatalogHandle.fromId("bla:normal:1")),
                                                                Optional.of(TestingConnectorTransactionHandle.INSTANCE),
                                                                new TestingPartitioningHandle()),
                                                        ImmutableList.of(new Symbol(BIGINT, "f"))),
                                                ImmutableList.of(new Symbol(BIGINT, "f"), new Symbol(BOOLEAN, "g")),
                                                false,
                                                Optional.of(new int[] {5, 6, 7}),
                                                OptionalInt.empty(),
                                                OptionalInt.empty()),
                                        ImmutableList.of(
                                                new ValuesNode(
                                                        new PlanNodeId("102"),
                                                        ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                        ImmutableList.of(new Row(ImmutableList.of(new Constant(BIGINT, 3L), new Constant(BOOLEAN, true))))),
                                                new ValuesNode(
                                                        new PlanNodeId("103"),
                                                        ImmutableList.of(new Symbol(SMALLINT, "c"), new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e")),
                                                        ImmutableList.of())),
                                        ImmutableList.of(
                                                ImmutableList.of(new Symbol(BIGINT, "a"), new Symbol(BOOLEAN, "b")),
                                                ImmutableList.of(new Symbol(BIGINT, "d"), new Symbol(BOOLEAN, "e"))),
                                        Optional.empty()),
                                ImmutableList.of("col_a", "col_b"),
                                ImmutableList.of(new Symbol(BIGINT, "f"), new Symbol(BOOLEAN, "g"))))
                .print(TESTING_PRINT_OPTIONS))
                .isEqualTo(
                        """
                        IR version = 1
                        %0 = query() : () -> "boolean" ({
                            ^query
                                %1 = values() : () -> "multiset(row(bigint,boolean))" ({
                                    ^row
                                        %2 = constant() : () -> "bigint" ()
                                            {constant:value = "[type=bigint, value=3]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %3 = constant() : () -> "boolean" ()
                                            {constant:value = "[type=boolean, value=true]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %4 = row(%2, %3) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %5 = return(%4) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {values:cardinality = "1", values:row_type = "row(bigint,boolean)", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                %6 = values() : () -> "multiset(row(smallint,bigint,boolean))" ()
                                    {values:cardinality = "0", values:row_type = "row(smallint,bigint,boolean)", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                %7 = exchange(%1, %6) : ("multiset(row(bigint,boolean))", "multiset(row(smallint,bigint,boolean))") -> "multiset(row(bigint,boolean))" ({
                                    ^inputSelector (%8 : "row(bigint,boolean)")
                                        %9 = field_reference(%8) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %10 = field_reference(%8) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %11 = row(%9, %10) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %12 = return(%11) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    }, {
                                    ^inputSelector (%13 : "row(smallint,bigint,boolean)")
                                        %14 = field_reference(%13) : ("row(smallint,bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %15 = field_reference(%13) : ("row(smallint,bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "2", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %16 = row(%14, %15) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %17 = return(%16) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    }, {
                                    ^boundArguments (%18 : "row(bigint,boolean)")
                                        %19 = field_reference(%18) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %20 = row(%19) : ("bigint") -> "row(bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %21 = return(%20) : ("row(bigint)") -> "row(bigint)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    }, {
                                    ^orderingSelector (%22 : "row(bigint,boolean)")
                                        %23 = constant() : () -> "empty row" ()
                                            {constant:value = "[type=empty row, value=null]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %24 = return(%23) : ("empty row") -> "empty row" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {exchange:type = "GATHER", exchange:scope = "REMOTE", exchange:partitioning_handle = "bla:testing partitioning handle", exchange:constant_values = "[null]", exchange:replicate_nulls_and_any = "false", exchange:bucket_to_partition = "[5,6,7]", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                %25 = output(%7) : ("multiset(row(bigint,boolean))") -> "boolean" ({
                                    ^outputFieldSelector (%26 : "row(bigint,boolean)")
                                        %27 = field_reference(%26) : ("row(bigint,boolean)") -> "bigint" ()
                                            {field_reference:index = "0", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %28 = field_reference(%26) : ("row(bigint,boolean)") -> "boolean" ()
                                            {field_reference:index = "1", ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %29 = row(%27, %28) : ("bigint", "boolean") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true"}
                                        %30 = return(%29) : ("row(bigint,boolean)") -> "row(bigint,boolean)" ()
                                            {ir.has_side_effects = "false", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                                    })
                                    {output:column_names = "[""col_a"",""col_b""]", ir.has_side_effects = "true", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                            })
                            {ir.has_side_effects = "true", ir.repeatability = "DETERMINISTIC", ir.safe = "true", ir.terminal = "true"}
                        """);
    }

    private enum TestingConnectorTableHandle
            implements ConnectorTableHandle
    {
        INSTANCE
    }

    private static class TestingPartitioningHandle
            implements ConnectorPartitioningHandle
    {
        @Override
        public String toString()
        {
            return "testing partitioning handle";
        }
    }
}
