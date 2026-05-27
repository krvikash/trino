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
package io.trino.operator.gpu;

import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.Cuda;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.operator.ReferenceCount;
import io.trino.operator.gpu.expression.CompiledExpression;
import io.trino.operator.gpu.expression.GpuExpression;
import io.trino.operator.gpu.expression.GpuIsNull;
import io.trino.operator.gpu.expression.GpuLogicalExpression;
import io.trino.operator.project.InputChannels;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.gpu.Column.Blocks;
import io.trino.spi.gpu.GpuTypeConversion;
import io.trino.spi.gpu.GpuTypeConversion.ToColumn;
import io.trino.spi.gpu.borrow.Borrow;
import io.trino.spi.gpu.borrow.Move;
import io.trino.spi.gpu.borrow.Own;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Verify.verifyNotNull;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.operator.gpu.expression.GpuExpressionCompiler.compileExpression;
import static java.util.Objects.requireNonNull;

public class GpuDynamicFilterProvider
{
    private static final Logger log = Logger.get(GpuDynamicFilterProvider.class);

    sealed interface CompiledDynamicFilter
    {
        record All()
                implements CompiledDynamicFilter {}

        record None()
                implements CompiledDynamicFilter {}

        record Expression(CompiledExpression expression)
                implements CompiledDynamicFilter
        {
            public Expression
            {
                requireNonNull(expression, "expression is null");
            }
        }
    }

    private final DomainTranslator domainTranslator;
    private final DynamicFilter dynamicFilter;
    private final Map<ColumnHandle, Integer> sourceLayout;
    private final Map<ColumnHandle, Type> handleType;
    private final Map<ColumnHandle, ToColumn> handleCopier;

    @GuardedBy("this")
    private TupleDomain<ColumnHandle> currentPredicate = TupleDomain.all();
    @GuardedBy("this")
    private boolean isDynamicFilterComplete;
    @GuardedBy("this")
    private VersionedFilter currentFilter = VersionedFilter.createAllFilter();

    private final ReferenceCount operatorReferences = new ReferenceCount(1 /* for filter operator factory */);

    public GpuDynamicFilterProvider(
            DomainTranslator domainTranslator,
            DynamicFilter dynamicFilter,
            Map<ColumnHandle, Integer> sourceLayout,
            List<Type> types)
    {
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.sourceLayout = ImmutableMap.copyOf(requireNonNull(sourceLayout, "sourceLayout is null"));
        this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");

        ImmutableMap.Builder<ColumnHandle, Type> handleType = ImmutableMap.builder();
        ImmutableMap.Builder<ColumnHandle, ToColumn> handleCopier = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, Integer> entry : sourceLayout.entrySet()) {
            ColumnHandle handle = entry.getKey();
            int channel = entry.getValue();
            Type type = types.get(channel);
            handleType.put(handle, type);
            handleCopier.put(handle, GpuTypeConversion.toGpuMapping(type)
                    .orElseThrow(() -> new UnsupportedOperationException("Unsupported type: " + type))
                    .toColumn());
        }
        this.handleType = handleType.buildOrThrow();
        this.handleCopier = handleCopier.buildOrThrow();

        operatorReferences.getFreeFuture().addListener(this::releaseCurrentVersion, directExecutor());
    }

    private synchronized void releaseCurrentVersion()
    {
        if (currentFilter != null) {
            currentFilter.referenceCount.release();
            currentFilter = null;
        }
    }

    <R> R useCurrentFilter(Function<@Borrow CompiledDynamicFilter, R> action)
    {
        VersionedFilter versionToRelease = null;
        VersionedFilter versionToUse = null;
        try {
            synchronized (this) {
                verifyNotNull(currentPredicate, "currentPredicate is null");
                verifyNotNull(currentFilter, "currentVersion is null");
                if (!isDynamicFilterComplete) {
                    boolean isComplete = dynamicFilter.isComplete();
                    TupleDomain<ColumnHandle> newPredicate = dynamicFilter.getCurrentPredicate();
                    if (!newPredicate.equals(currentPredicate)) {
                        versionToRelease = currentFilter;
                        // Temporarily unset so that we don't expose invalid state (e.g. closed currentVersion) in case of an exception
                        currentPredicate = null;
                        currentFilter = null;

                        VersionedFilter newFilter = compileTupleDomain(newPredicate);
                        // Sync to ensure full visibility of backing vectors for consuming threads.
                        Cuda.DEFAULT_STREAM.sync();
                        currentFilter = newFilter;
                        currentPredicate = newPredicate;
                    }
                    isDynamicFilterComplete = isComplete;
                }
                // Increment reference count still under lock. `this.currentVersion` is guaranteed to be live,
                // but the `versionToUse` is not guaranteed to be live the moment we leave the synchronized block.
                versionToUse = currentFilter;
                versionToUse.referenceCount.retain();
            }

            if (versionToRelease != null) {
                versionToRelease.referenceCount.release();
            }

            return action.apply(versionToUse.compiled);
        }
        finally {
            if (versionToUse != null) {
                if (!versionToUse.backingVectors.isEmpty()) {
                    // Sync to ensure `versionToUse` is indeed no longer in use
                    Cuda.DEFAULT_STREAM.sync();
                }
                versionToUse.referenceCount.release();
            }
        }
    }

    void operatorFactoryDuplicated()
    {
        operatorReferences.retain();
    }

    void operatorCreated()
    {
        operatorReferences.retain();
    }

    void operatorClosed()
    {
        operatorReferences.release();
    }

    void noMoreOperators()
    {
        operatorReferences.release();
    }

    private @Move VersionedFilter compileTupleDomain(TupleDomain<ColumnHandle> predicate)
    {
        if (predicate.isAll()) {
            return VersionedFilter.createAllFilter();
        }
        if (predicate.isNone()) {
            return VersionedFilter.createNoneFilter();
        }

        Map<ColumnHandle, Domain> domains = predicate.getDomains().orElseThrow();
        @Own List<ColumnVector> ownedVectors = new ArrayList<>();
        try {
            ImmutableList.Builder<GpuExpression> domainFilters = ImmutableList.builder();
            List<Integer> inputChannels = new ArrayList<>();
            int compiledDomains = 0;
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                int channel = sourceLayout.get(entry.getKey());
                Optional<GpuExpression> compiledDomain = compileDomain(entry.getKey(), entry.getValue(), ownedVectors);
                if (compiledDomain.isPresent()) {
                    GpuExpression compiled = compiledDomain.get();
                    int mappedChannel = inputChannels.size();
                    inputChannels.add(channel);
                    // The combined expression requests all channels. Due to constraints of GpuExpressionCompiler, compileDomain returns GpuExpression that expects single channel.
                    domainFilters.add((positionCount, inputColumns) -> compiled.evaluate(positionCount, ImmutableList.of(inputColumns.get(mappedChannel))));
                    compiledDomains++;
                }
            }
            if (compiledDomains == 0) {
                return VersionedFilter.createAllFilter();
            }
            CompiledDynamicFilter.Expression compiledFilter = new CompiledDynamicFilter.Expression(new CompiledExpression(
                    compiledDomains == 1
                            ? getOnlyElement(domainFilters.build())
                            : GpuLogicalExpression.and(domainFilters.build()),
                    new InputChannels(inputChannels)));
            VersionedFilter versionedFilter = new VersionedFilter(ownedVectors, compiledFilter);
            ownedVectors.clear(); // ownership is transferred to versionedFilter
            return versionedFilter;
        }
        finally {
            closeColumnVectors(ownedVectors);
        }
    }

    private Optional<GpuExpression> compileDomain(ColumnHandle handle, Domain domain, List<ColumnVector> ownedVectors)
    {
        if (domain.getValues() instanceof SortedRangeSet sortedRangeSet &&
                sortedRangeSet.isDiscreteSet()) {
            // Fast, vectorized track for discrete domains. These tend to be big and the generic route requires boxing and processing each of the IN list values
            // separately.
            ColumnVector inList = handleCopier.get(handle).copyToDevice(new Blocks(ImmutableList.of(sortedRangeSet.getSortedRanges())));
            ownedVectors.add(inList);
            GpuExpression inPredicate = (_, inputColumns) -> getOnlyElement(inputColumns).contains(inList);
            if (domain.isNullAllowed()) {
                return Optional.of(GpuLogicalExpression.or(ImmutableList.of(
                        inPredicate,
                        new GpuIsNull((_, inputColumns) -> getOnlyElement(inputColumns)))));
            }
            return Optional.of(inPredicate);
        }

        Symbol symbol = new Symbol(handleType.get(handle), "synthetic");
        Expression expression = domainTranslator.toPredicate(domain, symbol.toSymbolReference());
        Optional<GpuExpression> compiled = compileExpression(expression, ImmutableMap.of(symbol, 0))
                .map(CompiledExpression::expression);
        if (compiled.isEmpty()) {
            log.debug("Could not convert Domain to GPU expression: %s", domain);
        }
        return compiled;
    }

    private static final class VersionedFilter
    {
        static VersionedFilter createAllFilter()
        {
            return new VersionedFilter(ImmutableList.of(), new CompiledDynamicFilter.All());
        }

        static VersionedFilter createNoneFilter()
        {
            return new VersionedFilter(ImmutableList.of(), new CompiledDynamicFilter.None());
        }

        private final @Own List<ColumnVector> backingVectors;
        private final CompiledDynamicFilter compiled;
        private final ReferenceCount referenceCount = new ReferenceCount(1); // factory holds initial ref

        VersionedFilter(@Move List<ColumnVector> backingVectors, CompiledDynamicFilter compiled)
        {
            this.backingVectors = ImmutableList.copyOf(requireNonNull(backingVectors, "backingVectors is null"));
            this.compiled = requireNonNull(compiled, "compiled is null");
            referenceCount.getFreeFuture().addListener(this::closeVectors, directExecutor());
        }

        private void closeVectors()
        {
            closeColumnVectors(backingVectors);
        }
    }

    private static void closeColumnVectors(List<ColumnVector> vectors)
    {
        for (ColumnVector v : vectors) {
            v.close();
        }
    }
}
