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

import io.airlift.log.Logger;
import io.trino.plugin.warp.dispatcher.query.MatchCollectUtils;
import io.trino.plugin.warp.dispatcher.query.PredicateData;
import io.trino.plugin.warp.dispatcher.query.PredicateInfo;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.collect.PrefilledQueryCollectData;
import io.trino.plugin.warp.dispatcher.query.data.match.BasicQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.LuceneQueryMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.MatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.juffer.PredicateCacheData;
import io.trino.plugin.warp.juffer.PredicatesCacheService;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.plugin.warp.log.ShapingLoggerFactory;
import io.trino.plugin.warp.storage.engine.StorageEngineConstants;
import io.trino.plugin.warp.tools.util.Pair;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.warp.dispatcher.query.MatchCollectUtils.canMatchForMatchCollect;
import static io.trino.plugin.warp.dispatcher.query.classifier.PredicateUtil.calcPredicateData;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_ALL;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_LUCENE;
import static io.trino.plugin.warp.gen.constants.PredicateType.PREDICATE_TYPE_NONE;
import static java.lang.String.format;

class PredicateBufferClassifier
        implements Classifier
{
    private static final Logger logger = Logger.get(PredicateBufferClassifier.class);

    private final ShapingLogger shapingLogger;
    private final PredicatesCacheService predicatesCacheService;
    private final StorageEngineConstants storageEngineConstants;
    private final PredefinedPredicate noneWithNulls;
    private final PredefinedPredicate noneWithoutNulls;
    private final PredefinedPredicate allWithNulls;
    private final PredefinedPredicate allWithoutNulls;
    private final PredefinedPredicate luceneWithNulls;
    private final PredefinedPredicate luceneWithoutNulls;

    PredicateBufferClassifier(
            PredicatesCacheService predicatesCacheService,
            ShapingLoggerFactory shapingLoggerFactory,
            StorageEngineConstants storageEngineConstants)
    {
        this.predicatesCacheService = predicatesCacheService;
        this.storageEngineConstants = storageEngineConstants;
        this.noneWithNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_NONE, true);
        this.noneWithoutNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_NONE, false);
        this.allWithNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_ALL, true);
        this.allWithoutNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_ALL, false);
        this.luceneWithNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_LUCENE, true);
        this.luceneWithoutNulls = buildPredicateDataWithoutBuffer(PREDICATE_TYPE_LUCENE, false);
        this.shapingLogger = shapingLoggerFactory.getInstance(this.getClass());
    }

    @Override
    public QueryContext classify(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        Map<Integer, PrefilledQueryCollectData> newPrefilledQueryCollectDataByBlockIndex = new HashMap<>(queryContext.getPrefilledQueryCollectDataByBlockIndex());
        Map<Integer, ColumnHandle> newRemainingCollectColumnByBlockIndex = new HashMap<>(queryContext.getRemainingCollectColumnByBlockIndex());
        Deque<NativeQueryCollectData> newNativeQueryCollectDataQueue = new ArrayDeque<>(queryContext.getNativeQueryCollectDataList());
        int columnIx = 0;

        Optional<MatchData> matchDataWithPredicateBuffers = queryContext.getMatchData().map(matchData -> calcMatchDataWithLogical(
                classifyArgs,
                matchData,
                queryContext,
                columnIx,
                newRemainingCollectColumnByBlockIndex,
                newPrefilledQueryCollectDataByBlockIndex,
                newNativeQueryCollectDataQueue));

        return queryContext.asBuilder()
                .matchData(matchDataWithPredicateBuffers)
                .prefilledQueryCollectDataByBlockIndex(newPrefilledQueryCollectDataByBlockIndex)
                .remainingCollectColumnByBlockIndex(newRemainingCollectColumnByBlockIndex)
                .nativeQueryCollectDataList(newNativeQueryCollectDataQueue.stream().toList())
                .build();
    }

    private QueryMatchData createPredicateBuffer(
            QueryContext queryContext,
            ClassifyArgs classifyArgs,
            QueryMatchData queryMatchData,
            Map<Integer, ColumnHandle> newRemainingCollectColumnByBlockIndex,
            Map<Integer, PrefilledQueryCollectData> newPrefilledQueryCollectDataByBlockIndex,
            Deque<NativeQueryCollectData> newNativeQueryCollectDataQueue)
    {
        boolean simplifiedDomain = queryMatchData.isSimplifiedDomain();
        boolean tightnessRequired = queryMatchData.isTightnessRequired();
        boolean collectNulls;
        Pair<PredicateCacheData, Boolean> queryMatch;
        Type columnType = queryMatchData.getType();
        Optional<Domain> optionalDomain;
        PredicateCacheData predicateCacheData;
        Domain domain = queryMatchData.getDomain().orElse(Domain.all(queryMatchData.getType()));
        Optional<NativeExpression> nativeExpressionOptional = queryMatchData instanceof BasicQueryMatchData basicQueryMatchData ? Optional.of(basicQueryMatchData.getNativeExpression()) : Optional.empty();
        if (queryMatchData instanceof LuceneQueryMatchData) {
            PredefinedPredicate predefinedPredicate = queryMatchData.isCollectNulls() ? luceneWithNulls : luceneWithoutNulls;
            predicateCacheData = predefinedPredicate.predicateCacheData;
            collectNulls = predefinedPredicate.collectNulls;
        }
        else if (domain.getValues().isAll()) {
            PredefinedPredicate predefinedPredicate = domain.isNullAllowed() ? allWithNulls : allWithoutNulls;
            predicateCacheData = predefinedPredicate.predicateCacheData;
            collectNulls = predefinedPredicate.collectNulls;
        }
        else if (domain.getValues().isNone()) {
            PredefinedPredicate predefinedPredicate = domain.isNullAllowed() ? noneWithNulls : noneWithoutNulls;
            predicateCacheData = predefinedPredicate.predicateCacheData;
            collectNulls = predefinedPredicate.collectNulls;
        }
        else {
            WarmUpType warmUpType = queryMatchData.getWarmUpElement().getWarmUpType();
            boolean transformAllowed = warmUpType == WarmUpType.WARM_UP_TYPE_BASIC &&
                    !MatchCollectUtils.canBeMatchForMatchCollect(queryMatchData, queryContext.getNativeQueryCollectDataList());
            PredicateData predicateData;
            int recTypeLength = queryMatchData.getWarmUpElement().getRecTypeLength();
            if (nativeExpressionOptional.isPresent()) {
                domain = nativeExpressionOptional.get().domain();
                // CAST/DAY-WEEK predicates size off the domain's type width, not recTypeLength - see usesDomainWidth().
                int functionTargetRecTypeLength = PredicateUtil.usesDomainWidth(nativeExpressionOptional.get().functionType())
                        ? TypeUtils.getTypeLength(domain.getType(), storageEngineConstants.getVarcharMaxLen())
                        : recTypeLength;
                predicateData = calcPredicateData(
                        nativeExpressionOptional.get(),
                        recTypeLength,
                        transformAllowed,
                        columnType,
                        functionTargetRecTypeLength);
            }
            else {
                predicateData = calcPredicateData(
                        domain,
                        recTypeLength,
                        transformAllowed,
                        columnType);
            }
            collectNulls = predicateData.isCollectNulls();
            queryMatch = calculatePredicateBuffer(predicateData, queryMatchData, newPrefilledQueryCollectDataByBlockIndex, newRemainingCollectColumnByBlockIndex, newNativeQueryCollectDataQueue, classifyArgs, domain, queryContext, tightnessRequired, domain.isNullAllowed());
            if (!queryMatch.getRight()) {
                tightnessRequired = false;
                simplifiedDomain = true;  // A simplified domain will cause the entire queryContext to be marked with canBeTight = false
                collectNulls = domain.isNullAllowed();
            }
            predicateCacheData = queryMatch.getLeft();
        }
        optionalDomain = Optional.of(domain);
        return queryMatchData.asBuilder()
                .collectNulls(collectNulls)
                .predicateCacheData(predicateCacheData)
                .tightnessRequired(tightnessRequired)
                .simplifiedDomain(simplifiedDomain)
                .domain(optionalDomain)
                .build();
    }

    private Pair<PredicateCacheData, Boolean> calculatePredicateBuffer(
            PredicateData predicateData,
            QueryMatchData queryMatchData,
            Map<Integer, PrefilledQueryCollectData> newPrefilledQueryCollectDataByBlockIndex,
            Map<Integer, ColumnHandle> newRemainingCollectColumnByBlockIndex,
            Deque<NativeQueryCollectData> newNativeQueryCollectDataQueue,
            ClassifyArgs classifyArgs,
            Domain domain,
            QueryContext queryContext,
            boolean tightnessRequired,
            boolean isNullAllowed)
    {
        PredicateCacheData predicateCacheData;
        boolean allocatedBuffer;
        Optional<PredicateCacheData> predicateCacheDataOpt = classifyArgs.isDebugNoPredicateBuffer() ?
                Optional.empty() :
                predicatesCacheService.getOrCreatePredicateBufferId(predicateData, domain);
        if (predicateCacheDataOpt.isPresent()) {
            predicateCacheData = predicateCacheDataOpt.get();
            if (queryMatchData.canMapMatchCollect() && predicateCacheData.getValuesDict().isEmpty()) {
                rollbackMapMatchCollect(queryContext, queryMatchData, classifyArgs, newNativeQueryCollectDataQueue, newRemainingCollectColumnByBlockIndex);
                shapingLogger.warn("didnt get mapping for mappedMatchCollect. PredicateData %s queryMatchData %s domain %s queryContext %s", predicateData, queryMatchData, domain, queryContext);
            }

            allocatedBuffer = true;
        }
        else {
            // no buffer is available. we fall back to predicate all and return a result to be filtered by upper layers
            PredefinedPredicate predefinedPredicate = isNullAllowed ? allWithNulls : allWithoutNulls;
            predicateCacheData = predefinedPredicate.predicateCacheData;
            if (tightnessRequired) {
                logger.debug(
                        "Tightness requirement can't be met because no predicate buffer is available. " +
                                "predicateData=%s, queryMatchData=%s, queryContext=%s",
                        predicateData,
                        queryMatchData,
                        queryContext);
                // Not tight anymore - remove prefill.
                // Theoretically, we can try to find another nativeQueryMatchData of the same column with canBeTight() = true,
                // but since we currently choose only one nativeQueryMatchData of type lucene \ basic per column, we'll actually need to create it.
                // Since this is an edge case and a better solution is not simple, just rollback the prefill creation - to avoid bugs.
                queryContext.getPrefilledQueryCollectDataByBlockIndex()
                        .entrySet().stream()
                        .filter(entry -> entry.getValue().getWarpColumn().equals(queryMatchData.getWarpColumn()))
                        .forEach(entry -> {
                            int blockIndex = entry.getKey();
                            newPrefilledQueryCollectDataByBlockIndex.remove(blockIndex);
                            newRemainingCollectColumnByBlockIndex.put(blockIndex, classifyArgs.getCollectColumn(blockIndex));
                        });
            }

            if (queryMatchData.canMapMatchCollect()) {
                rollbackMapMatchCollect(queryContext, queryMatchData, classifyArgs, newNativeQueryCollectDataQueue, newRemainingCollectColumnByBlockIndex);
            }
            allocatedBuffer = false;
        }
        return Pair.of(predicateCacheData, allocatedBuffer);
    }

    private void rollbackMapMatchCollect(
            QueryContext queryContext,
            QueryMatchData queryMatchData,
            ClassifyArgs classifyArgs,
            Deque<NativeQueryCollectData> newNativeQueryCollectDataQueue,
            Map<Integer, ColumnHandle> newRemainingCollectColumnByBlockIndex)
    {
        List<NativeQueryCollectData> mappedMatchCollects = queryContext.getNativeQueryCollectDataList().stream()
                .filter(collectData -> collectData.getMatchCollectType().equals(MatchCollectUtils.MatchCollectType.MAPPED) &&
                        canMatchForMatchCollect(queryMatchData, collectData)).toList();

        // Map match collect is not supported with predicate functions, so there can be at most one pair of mappedMatchCollects
        // for a single match element or collect element.
        checkState(mappedMatchCollects.size() <= 1, "Should not have more then 1 collect for a single match. got %s", mappedMatchCollects.size());
        if (!mappedMatchCollects.isEmpty()) {
            NativeQueryCollectData collectData = mappedMatchCollects.getFirst();
            int blockIndex = collectData.getBlockIndex();
            newRemainingCollectColumnByBlockIndex.put(blockIndex, classifyArgs.getCollectColumn(blockIndex));
            newNativeQueryCollectDataQueue.remove(collectData);
        }
    }

    private MatchData calcMatchDataWithLogical(
            ClassifyArgs classifyArgs,
            MatchData matchData,
            QueryContext queryContext,
            int columnIx,
            Map<Integer, ColumnHandle> newRemainingCollectColumnByBlockIndex,
            Map<Integer, PrefilledQueryCollectData> newPrefilledQueryCollectDataByBlockIndex,
            Deque<NativeQueryCollectData> newNativeQueryCollectDataQueue)
    {
        MatchData res;
        if (matchData instanceof LogicalMatchData logicalMatchData) {
            List<MatchData> terms = logicalMatchData.getTerms();
            List<MatchData> newTerms = new ArrayList<>();
            for (MatchData term : terms) {
                MatchData newTerm = calcMatchDataWithLogical(
                        classifyArgs,
                        term,
                        queryContext,
                        columnIx,
                        newRemainingCollectColumnByBlockIndex,
                        newPrefilledQueryCollectDataByBlockIndex,
                        newNativeQueryCollectDataQueue);
                newTerms.add(newTerm);
                columnIx += newTerm.getLeavesDFS().size();
            }
            res = new LogicalMatchData(logicalMatchData.getOperator(), newTerms);
        }
        else if (matchData instanceof QueryMatchData queryMatchData) {
            res = createPredicateBuffer(
                    queryContext,
                    classifyArgs,
                    queryMatchData,
                    newRemainingCollectColumnByBlockIndex,
                    newPrefilledQueryCollectDataByBlockIndex,
                    newNativeQueryCollectDataQueue);
        }
        else {
            throw new UnsupportedOperationException(format("unsupported MatchData type %s", matchData));
        }
        return res;
    }

    private PredefinedPredicate buildPredicateDataWithoutBuffer(PredicateType predicateType, boolean isCollectNulls)
    {
        int numMatchElements = 0;
        PredicateInfo predicateInfo = new PredicateInfo(predicateType, FunctionType.FUNCTION_TYPE_NONE, numMatchElements, Collections.emptyList(), 0);

        PredicateData predicateData = PredicateData
                .builder()
                .isCollectNulls(isCollectNulls)
                .predicateInfo(predicateInfo)
                .predicateHashCode(0)
                .predicateSize(PredicateUtil.PREDICATE_HEADER_SIZE)
                .columnType(IntegerType.INTEGER) // fake - unused
                .build();
        PredicateCacheData predicateCacheData = predicatesCacheService.predicateDataToBuffer(predicateData, null).get();
        return new PredefinedPredicate(isCollectNulls, predicateCacheData);
    }

    private record PredefinedPredicate(
            @SuppressWarnings("unused") boolean collectNulls,
            @SuppressWarnings("unused") PredicateCacheData predicateCacheData) {}
}
