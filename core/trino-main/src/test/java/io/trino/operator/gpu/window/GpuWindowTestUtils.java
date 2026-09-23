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
package io.trino.operator.gpu.window;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.gpu.GpuOperation;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.sql.planner.OrderingScheme;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.DataOrganizationSpecification;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import io.trino.sql.planner.plan.WindowNode;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;

public final class GpuWindowTestUtils
{
    private GpuWindowTestUtils() {}

    public static final List<String> TESTED_GPU_WINDOW_FUNCTIONS = List.of("row_number", "rank", "dense_rank");

    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    public static GpuWindowCompiler.CompileResult compileWindow(
            List<Type> sourceTypes,
            int[] partitionChannels,
            int[] orderChannels,
            List<SortOrder> orderSortOrders,
            List<String> functions)
    {
        Symbol[] sourceSymbols = new Symbol[sourceTypes.size()];
        for (int i = 0; i < sourceTypes.size(); i++) {
            sourceSymbols[i] = new Symbol(sourceTypes.get(i), "c" + i);
        }

        ImmutableList.Builder<Symbol> partitionByBuilder = ImmutableList.builder();
        for (int channel : partitionChannels) {
            partitionByBuilder.add(sourceSymbols[channel]);
        }
        List<Symbol> partitionBy = partitionByBuilder.build();

        Optional<OrderingScheme> orderingScheme = Optional.empty();
        if (orderChannels.length > 0) {
            ImmutableList.Builder<Symbol> orderByBuilder = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, SortOrder> orderingsBuilder = ImmutableMap.builder();
            for (int i = 0; i < orderChannels.length; i++) {
                orderByBuilder.add(sourceSymbols[orderChannels[i]]);
                orderingsBuilder.put(sourceSymbols[orderChannels[i]], orderSortOrders.get(i));
            }
            orderingScheme = Optional.of(new OrderingScheme(orderByBuilder.build(), orderingsBuilder.buildOrThrow()));
        }

        ImmutableMap.Builder<Symbol, WindowNode.Function> windowFunctions = ImmutableMap.builder();
        for (int i = 0; i < functions.size(); i++) {
            windowFunctions.put(new Symbol(BIGINT, "f" + i), rankingFunction(functions.get(i)));
        }

        WindowNode node = windowNode(sourceSymbols, partitionBy, orderingScheme, windowFunctions.buildOrThrow());

        ImmutableMap.Builder<Symbol, Integer> layout = ImmutableMap.builder();
        for (int i = 0; i < sourceSymbols.length; i++) {
            layout.put(sourceSymbols[i], i);
        }
        return GpuWindowCompiler.compile(node, layout.buildOrThrow(), sourceTypes);
    }

    private static WindowNode windowNode(Symbol[] sourceOutputs, List<Symbol> partitionBy, Optional<OrderingScheme> orderingScheme, Map<Symbol, WindowNode.Function> functions)
    {
        return new WindowNode(
                new PlanNodeId("test"),
                new ValuesNode(new PlanNodeId("source"), Arrays.asList(sourceOutputs), ImmutableList.of()),
                new DataOrganizationSpecification(partitionBy, orderingScheme),
                ImmutableMap.copyOf(functions),
                ImmutableSet.of(),
                0);
    }

    private static WindowNode.Function rankingFunction(String name)
    {
        ResolvedFunction resolvedFunction = FUNCTION_RESOLUTION.resolveFunction(name, List.of());
        return new WindowNode.Function(
                resolvedFunction,
                List.of(),
                Optional.empty(),
                WindowNode.Frame.DEFAULT_FRAME,
                false,
                false);
    }

    public static GpuOperation createOperation(GpuOperation.Context context, GpuOperation source, List<GpuOperation.Factory> stages)
    {
        GpuOperation operation = source;
        for (GpuOperation.Factory stage : stages) {
            operation = stage.create(context, operation);
        }
        return operation;
    }
}
