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
package io.trino.plugin.warp.dispatcher.query.classifier;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.model.TransformedColumn;
import io.trino.plugin.warp.dispatcher.model.WarmUpElement;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.query.PredicateContext;
import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.dispatcher.query.data.match.BasicQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.juffer.BufferAllocator;
import io.trino.plugin.warp.juffer.PredicateBufferPoolType;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.plugin.warp.dispatcher.query.classifier.PredicateUtil.calcPredicateData;
import static io.trino.plugin.warp.dispatcher.query.classifier.PredicateUtil.canApplyPredicate;
import static io.trino.plugin.warp.dispatcher.query.classifier.PredicateUtil.usesDomainWidth;
import static java.util.Objects.requireNonNull;

class BasicMatcher
        implements Matcher
{
    private static final Logger logger = Logger.get(BasicMatcher.class);

    private final BufferAllocator bufferAllocator;
    private final StorageEngineConstants storageEngineConstants;

    BasicMatcher(BufferAllocator bufferAllocator, StorageEngineConstants storageEngineConstants)
    {
        this.bufferAllocator = requireNonNull(bufferAllocator);
        this.storageEngineConstants = requireNonNull(storageEngineConstants);
    }

    @Override
    public MatchContext match(
            ClassifyArgs classifyArgs,
            MatchContext matchContext)
    {
        ImmutableListMultimap<WarpColumn, WarmUpElement> basicColNameToWarmupElement = classifyArgs.getWarmedWarmupTypes().basicWarmedElements();
        if (basicColNameToWarmupElement.isEmpty()) {
            return matchContext;
        }
        List<QueryMatchData> matchDataList = new ArrayList<>(matchContext.matchDataList());
        ImmutableMap.Builder<WarpColumn, PredicateContext> remainingPredicateContext = ImmutableMap.builder();

        for (Map.Entry<WarpColumn, PredicateContext> predicateColumn : matchContext.remainingPredicateContext().entrySet()) {
            PredicateContext predicateContext = predicateColumn.getValue();
            Optional<NativeExpression> nativeExpression = predicateContext.getNativeExpression();
            Optional<WarmUpElement> warmUpElement;
            Domain domain = predicateContext.getDomain();

            if (nativeExpression.isPresent() && !domain.isAll()) {
                List<WarmUpElement> existingWarmupElements = basicColNameToWarmupElement.get(predicateColumn.getKey());
                warmUpElement = existingWarmupElements.stream()
                        .filter(element ->
                                ((element.getWarpColumn() instanceof TransformedColumn transformedColumn) &&
                                        Objects.equals(transformedColumn.getTransformFunction(), nativeExpression.get().transformFunction())) ||
                                        (!element.getWarpColumn().isTransformedColumn() &&
                                                Objects.equals(nativeExpression.get().transformFunction(), TransformFunction.NONE)))
                        .findFirst();

                if (warmUpElement.isPresent() &&
                        canApplyPredicate(warmUpElement, predicateContext.getColumnType()) &&
                        predicateFitsBufferPool(nativeExpression.get(), predicateContext.getColumnType(), warmUpElement.get())) {
                    matchDataList.add(BasicQueryMatchData.builder()
                            .warmUpElement(warmUpElement.get())
                            .type(predicateContext.getColumnType())
                            .domain(Optional.of(domain))
                            .simplifiedDomain(predicateContext.isSimplified())
                            .nativeExpression(nativeExpression.get())
                            .tightnessRequired(classifyArgs.getDispatcherTableHandle().isSubsumedPredicates())
                            .build());
                }
                else {
                    remainingPredicateContext.put(predicateColumn.getKey(), predicateContext);
                }
            }
            else {
                remainingPredicateContext.put(predicateColumn.getKey(), predicateContext);
            }
        }
        return new MatchContext(matchDataList, remainingPredicateContext.buildOrThrow(), true);
    }

    // predicates too large for any predicate buffer pool would degrade to a predicate-ALL match
    // in PredicateBufferClassifier (full index-match cost, zero filtering), so leave them unmatched
    // to be filtered externally
    private boolean predicateFitsBufferPool(NativeExpression nativeExpression, Type columnType, WarmUpElement warmUpElement)
    {
        int recTypeLength = warmUpElement.getRecTypeLength();
        try {
            int functionTargetRecTypeLength;
            if (usesDomainWidth(nativeExpression.functionType())) {
                functionTargetRecTypeLength = TypeUtils.getTypeLength(nativeExpression.domain().getType(), storageEngineConstants.getVarcharMaxLen());
            }
            else {
                functionTargetRecTypeLength = recTypeLength;
            }
            // transformAllowed=true yields the smallest possible representation, so a leaf is declined only when no representation fits
            PredicateData predicateData = calcPredicateData(nativeExpression, recTypeLength, true, columnType, functionTargetRecTypeLength);
            return bufferAllocator.getRequiredPredicateBufferType(predicateData.getPredicateSize()) != PredicateBufferPoolType.INVALID;
        }
        catch (RuntimeException e) {
            logger.debug(e, "predicate size calculation failed, keeping the basic match");
            return true;
        }
    }
}
