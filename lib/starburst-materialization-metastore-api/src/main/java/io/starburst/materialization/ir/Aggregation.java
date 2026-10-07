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

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.CatalogSchemaFunctionName;
import io.trino.spi.type.FunctionType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

/**
 * Mirrors the engine's {@code AggregationNode}. Its outputs are the grouping keys followed by the aggregation
 * outputs. Grouping keys pass source symbols through; aggregation outputs are new bindings, never bound anywhere
 * else in the tree, so a matcher can identify a binding by its symbol within one tree.
 */
@JsonTypeName(Aggregation.NAME)
public record Aggregation(
        Operation source,
        GroupingSets groupingSets,
        List<AggregateFunctionCall> aggregations,
        Step step,
        Optional<Symbol> groupIdSymbol)
        implements Operation
{
    static final String NAME = "Aggregation";
    static final int VERSION = 1;

    public Aggregation
    {
        requireNonNull(source, "source is null");
        requireNonNull(groupingSets, "groupingSets is null");
        aggregations = ImmutableList.copyOf(requireNonNull(aggregations, "aggregations is null"));
        requireNonNull(step, "step is null");
        requireNonNull(groupIdSymbol, "groupIdSymbol is null");

        Set<Symbol> sourceOutputs = ImmutableSet.copyOf(source.outputs());
        checkArgument(sourceOutputs.containsAll(groupingSets.groupingKeys()), "Aggregation: grouping key is not an output of the source");
        groupIdSymbol.ifPresent(symbol -> checkArgument(groupingSets.groupingKeys().contains(symbol), "Aggregation: group id symbol is not a grouping key"));

        Set<String> boundInSource = boundNames(source);
        Set<String> outputNames = new HashSet<>();
        for (AggregateFunctionCall aggregation : aggregations) {
            aggregation.verify(sourceOutputs, step);
            String outputName = aggregation.output().name();
            checkArgument(!boundInSource.contains(outputName), "Aggregation: aggregation output name is already bound in the source");
            checkArgument(outputNames.add(outputName), "Aggregation: aggregation output name is bound more than once");
        }
        checkArgument(
                step == Step.SINGLE || aggregations.stream().allMatch(aggregation -> aggregation.orderingScheme().isEmpty()),
                "Aggregation: ORDER BY requires step SINGLE");
    }

    @Override
    public List<Symbol> outputs()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(groupingSets.groupingKeys())
                .addAll(aggregations.stream().map(AggregateFunctionCall::output).iterator())
                .build();
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public List<IrNode> children()
    {
        ImmutableList.Builder<IrNode> children = ImmutableList.builder();
        children.add(source);
        aggregations.forEach(aggregation -> children.addAll(aggregation.arguments()));
        return children.build();
    }

    /**
     * Besides its bare name, an {@code Aggregation} stamps each aggregate function it persists: the decoder rejects a
     * function outside {@link SupportedAggregateFunctions}, so a reader that lacks one must skip the row. It also stamps
     * the leaf forms it embeds directly: unlike {@code Output} and {@code TableScan}, it postdates the leaf keys, so no
     * reader that knows it lacks them.
     */
    @Override
    public Map<String, Integer> versionEntries()
    {
        Map<String, Integer> entries = new HashMap<>(Operation.super.versionEntries());
        entries.put(IrVersions.SYMBOL_LEAF, IrVersions.SYMBOL_LEAF_VERSION);
        if (!aggregations.isEmpty()) {
            entries.put(IrVersions.BOUND_SIGNATURE_LEAF, IrVersions.BOUND_SIGNATURE_LEAF_VERSION);
        }
        for (AggregateFunctionCall aggregation : aggregations) {
            CatalogSchemaFunctionName function = aggregation.function().getName();
            entries.put(IrVersions.functionKey(function), SupportedAggregateFunctions.semanticVersion(function));
        }
        return ImmutableMap.copyOf(entries);
    }

    private static Set<String> boundNames(Operation operation)
    {
        return switch (operation) {
            case TableScan tableScan -> tableScan.assignments().values().stream()
                    .map(Symbol::name)
                    .collect(toImmutableSet());
            case Aggregation aggregate -> ImmutableSet.<String>builder()
                    .addAll(boundNames(aggregate.source()))
                    .addAll(aggregate.aggregations().stream().map(aggregation -> aggregation.output().name()).iterator())
                    .build();
            case Output output -> boundNames(output.source());
        };
    }

    public enum Step
    {
        SINGLE,
        PARTIAL,
        INTERMEDIATE,
        FINAL,
    }

    public record GroupingSets(List<Symbol> groupingKeys, int groupingSetCount, Set<Integer> globalGroupingSets)
    {
        public GroupingSets
        {
            groupingKeys = ImmutableList.copyOf(requireNonNull(groupingKeys, "groupingKeys is null"));
            globalGroupingSets = ImmutableSet.copyOf(requireNonNull(globalGroupingSets, "globalGroupingSets is null"));
            checkArgument(groupingSetCount > 0, "Aggregation: grouping set count must be positive");
            checkArgument(globalGroupingSets.size() <= groupingSetCount, "Aggregation: more global grouping sets than grouping sets");
            checkArgument(!groupingKeys.isEmpty() || !globalGroupingSets.isEmpty(), "Aggregation: no grouping keys requires a global grouping set");
        }
    }

    public record AggregateFunctionCall(
            Symbol output,
            BoundSignature function,
            List<Expression> arguments,
            boolean distinct,
            Optional<Symbol> filter,
            Optional<OrderingScheme> orderingScheme,
            Optional<Symbol> mask)
    {
        public AggregateFunctionCall
        {
            requireNonNull(output, "output is null");
            requireNonNull(function, "function is null");
            arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
            requireNonNull(filter, "filter is null");
            requireNonNull(orderingScheme, "orderingScheme is null");
            requireNonNull(mask, "mask is null");
            checkArgument(SupportedAggregateFunctions.contains(function.getName()), "Aggregation: function %s is not supported", function.getName());
            // The engine also accepts a lambda, which has no persisted form yet
            for (Expression argument : arguments) {
                checkArgument(argument instanceof Reference, "Aggregation: argument of function %s is not a reference", function.getName());
            }
        }

        private void verify(Set<Symbol> sourceOutputs, Step step)
        {
            // Intermediate and final steps take the intermediate state plus the lambda arguments
            int expectedArgumentCount = switch (step) {
                case SINGLE, PARTIAL -> function.getArity();
                case INTERMEDIATE, FINAL -> 1 + (int) function.getArgumentTypes().stream().filter(FunctionType.class::isInstance).count();
            };
            checkArgument(
                    arguments.size() == expectedArgumentCount,
                    "Aggregation: %s function %s expects %s arguments, got %s",
                    step,
                    function.getName(),
                    expectedArgumentCount,
                    arguments.size());
            for (Expression argument : arguments) {
                checkArgument(sourceOutputs.contains(((Reference) argument).symbol()), "Aggregation: argument of function %s is not an output of the source", function.getName());
            }
            filter.ifPresent(symbol -> checkArgument(sourceOutputs.contains(symbol), "Aggregation: filter of function %s is not an output of the source", function.getName()));
            mask.ifPresent(symbol -> checkArgument(sourceOutputs.contains(symbol), "Aggregation: mask of function %s is not an output of the source", function.getName()));
            orderingScheme.ifPresent(ordering -> {
                for (SortItem sortItem : ordering.sortItems()) {
                    checkArgument(sourceOutputs.contains(sortItem.symbol()), "Aggregation: sort item of function %s is not an output of the source", function.getName());
                }
            });
        }
    }

    public record OrderingScheme(List<SortItem> sortItems)
    {
        public OrderingScheme
        {
            sortItems = ImmutableList.copyOf(requireNonNull(sortItems, "sortItems is null"));
            checkArgument(!sortItems.isEmpty(), "Aggregation: ordering has no sort items");
            Set<Symbol> sortSymbols = new HashSet<>();
            for (SortItem sortItem : sortItems) {
                checkArgument(sortSymbols.add(sortItem.symbol()), "Aggregation: ordering sorts by a symbol more than once");
            }
        }
    }

    public record SortItem(Symbol symbol, SortOrder sortOrder)
    {
        public SortItem
        {
            requireNonNull(symbol, "symbol is null");
            requireNonNull(sortOrder, "sortOrder is null");
        }
    }
}
