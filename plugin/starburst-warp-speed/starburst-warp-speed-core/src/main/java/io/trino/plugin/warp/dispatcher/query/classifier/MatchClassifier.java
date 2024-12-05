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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.model.WarpColumn;
import io.trino.plugin.warp.dispatcher.query.PredicateContext;
import io.trino.plugin.warp.dispatcher.query.QueryContext;
import io.trino.plugin.warp.dispatcher.query.data.match.LogicalMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.MatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.NoneMatchData;
import io.trino.plugin.warp.dispatcher.query.data.match.QueryMatchData;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.gen.constants.WarmUpType;
import io.trino.plugin.warp.log.ShapingLogger;
import io.trino.spi.type.BooleanType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;

class MatchClassifier
        implements Classifier
{
    private static final Logger logger = Logger.get(MatchClassifier.class);
    private final ShapingLogger shapingLogger;

    private final List<Matcher> matchers;

    MatchClassifier(List<Matcher> matchers, GlobalConfig globalConfig)
    {
        this.matchers = matchers;
        this.shapingLogger = ShapingLogger.getInstance(
                logger,
                globalConfig.getShapingLoggerThreshold(),
                globalConfig.getShapingLoggerDuration(),
                globalConfig.getShapingLoggerNumberOfSamples());
    }

    @Override
    public QueryContext classify(ClassifyArgs classifyArgs, QueryContext queryContext)
    {
        PredicateContextData predicateContextData = queryContext.getPredicateContextData();
        ImmutableMap<WarpExpression, PredicateContext> leaves = predicateContextData.getLeaves();
        WarpExpression rootExpression = queryContext.getPredicateContextData().getRootExpression();
        if (rootExpression == WarpPrimitiveConstant.FALSE) {
            return queryContext.asBuilder()
                    .isNone(true)
                    .build();
        }
        if (leaves.isEmpty()) {
            return queryContext;
        }
        MatchResult matchResult = handleLogicalFunction(classifyArgs, leaves, rootExpression);
        boolean isNone = false;
        Set<WarpColumn> matchColumns = new HashSet<>();
        if (matchResult.matchData().isPresent()) {
            MatchData matchData = matchResult.matchData().get();
            if (matchData instanceof NoneMatchData) {
                isNone = true;
            }
            else {
                for (QueryMatchData queryMatchData : matchData.getLeavesDFS()) {
                    if (queryMatchData.getWarmUpElement().getWarmUpType() == WarmUpType.WARM_UP_TYPE_DATA) {
                        shapingLogger.error("calculated queryMatchData with WARM_UP_TYPE_DATA type, skip matching. matchData=%s. classifyArgs=%s", matchData, classifyArgs);
                        matchColumns.clear();
                        break;
                    }
                    WarpColumn warpColumn = queryMatchData.getWarpColumn();
                    matchColumns.add(warpColumn);
                }
            }
        }

        Map<WarpExpression, PredicateContext> remainingPredicateExpressions = isNone ?
                Collections.emptyMap() :
                leaves.entrySet()
                        .stream()
                        // TODO: This is not accurate, one match is not enough to determine that there are no remaining matches (for example in the case of domain + expression).
                        // TODO: This is ok for now since we mark canBeTight = false in the query context
                        .filter(entry -> !matchColumns.contains(entry.getValue().getWarpColumn()))
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // TODO: The predicate is not accurate anymore, leaves are changed while root remains as-is
        PredicateContextData remainingPredicateContextData = new PredicateContextData(ImmutableMap.copyOf(remainingPredicateExpressions), predicateContextData.getRootExpression());

        return queryContext.asBuilder()
                .matchData(matchResult.matchData())
                .predicateContextData(remainingPredicateContextData)
                .canBeTight(matchResult.canBeTight()) // Overwrite (instead of && with existing value) because beforehand, there might have been remaining predicates that caused queryContext to be non-tight
                .isNone(isNone)
                .build();
    }

    private MatchResult handleLogicalFunction(ClassifyArgs classifyArgs,
            Map<WarpExpression, PredicateContext> leaves,
            WarpExpression expression)
    {
        MatchResult result;

        try {
            if (expression instanceof WarpCall warpCall) {
                if (warpCall.getFunctionName().equals(AND_FUNCTION_NAME.getName())) {
                    result = handleAndExpression(classifyArgs, warpCall, leaves);
                }
                else if (warpCall.getFunctionName().equals(OR_FUNCTION_NAME.getName())) {
                    result = handleOrExpression(classifyArgs, warpCall, leaves);
                }
                else {
                    Optional<MatchData> matchData = handleFlatExpression(classifyArgs, leaves, expression);
                    result = new MatchResult(matchData, matchData.isPresent());
                }
            }
            else {
                Optional<MatchData> matchData = handleFlatExpression(classifyArgs, leaves, expression);
                result = new MatchResult(matchData, matchData.isPresent());
            }
        }
        catch (Exception e) {
            shapingLogger.warn(e, "failed on file %s", classifyArgs.getRowGroupData().getRowGroupKey());
            result = new MatchResult(Optional.empty(), false);
        }
        return result;
    }

    private Optional<MatchData> handleFlatExpression(ClassifyArgs classifyArgs,
            Map<WarpExpression, PredicateContext> leaves,
            WarpExpression expression)
    {
        Optional<MatchData> res;
        PredicateContext leaf = leaves.get(expression);
        if (leaf.getDomain().isNone()) {
            return Optional.of(new NoneMatchData());
        }
        if (!classifyArgs.isEnableInverseWithNulls() && leaf.isInverseWithNulls()) {
            return Optional.empty();
        }

        MatchContext matchContext = runMatchers(classifyArgs, Map.of(leaf.getWarpColumn(), leaf));
        if (!matchContext.validRange()) {
            // if there are any existing matches, they are dropped and replaced with none
            return Optional.of(new NoneMatchData());
        }
        List<MatchData> terms = new ArrayList<>(matchContext.matchDataList());
        if (terms.isEmpty()) {
            res = Optional.empty();
        }
        else if (terms.size() == 1) {
            res = Optional.of(terms.getFirst());
        }
        else {
            res = Optional.of(new LogicalMatchData(LogicalMatchData.Operator.AND, terms));
        }
        return res;
    }

    private MatchResult handleAndExpression(ClassifyArgs classifyArgs, WarpCall andExpression,
            Map<WarpExpression, PredicateContext> leaves)
    {
        List<MatchData> terms = new ArrayList<>();
        Map<WarpColumn, PredicateContext> remainingPredicateContext = new HashMap<>();
        boolean canBeTight = true;
        for (WarpExpression warpExpression : andExpression.getChildren()) {
            if (warpExpression instanceof WarpCall warpCall &&
                    (warpCall.getFunctionName().equals(OR_FUNCTION_NAME.getName()) ||
                            warpCall.getFunctionName().equals(AND_FUNCTION_NAME.getName()))) {
                MatchResult matchResult = handleLogicalFunction(classifyArgs, leaves, warpExpression);
                if (matchResult.matchData().isPresent()) {
                    MatchData matchData = matchResult.matchData().get();
                    if (matchData instanceof NoneMatchData) {
                        return new MatchResult(Optional.of(new NoneMatchData()), true);
                    }
                    terms.add(matchData);
                }
                else {
                    canBeTight = false;
                }
            }
            else {
                PredicateContext predicateContext = leaves.get(warpExpression);
                if (predicateContext.getDomain().isNone()) {
                    return new MatchResult(Optional.of(new NoneMatchData()), true);
                }
                WarpColumn warpColumn = predicateContext.getWarpColumn();
                PredicateContext existingPredicateContext = remainingPredicateContext.get(warpColumn);
                if (existingPredicateContext == null) {
                    remainingPredicateContext.put(warpColumn, predicateContext);
                }
                else {
                    Optional<PredicateContext> predicateContextBase = tryMergeAndPredicates(existingPredicateContext, predicateContext);
                    if (predicateContextBase.isPresent()) {
                        if (predicateContextBase.get().getDomain().isNone()) {
                            //no need to continue - will return emptyPageSource
                            return new MatchResult(Optional.of(new NoneMatchData()), true);
                        }
                        remainingPredicateContext.put(warpColumn, predicateContextBase.get());
                    }
                    else {
                        Optional<MatchData> matchData = handleFlatExpression(classifyArgs, leaves, warpExpression);
                        matchData.ifPresent(terms::add);
                    }
                }
            }
        }
        MatchContext matchContext = runMatchers(classifyArgs, remainingPredicateContext);
        if (!matchContext.validRange()) {
            return new MatchResult(Optional.of(new NoneMatchData()), true);
        }
        canBeTight = canBeTight && matchContext.remainingPredicateContext().isEmpty();
        terms.addAll(matchContext.matchDataList());
        Optional<MatchData> result;
        if (terms.isEmpty()) {
            result = Optional.empty();
        }
        else if (terms.size() == 1) {
            return new MatchResult(Optional.of(terms.getFirst()), canBeTight);
        }
        else {
            result = Optional.of(new LogicalMatchData(LogicalMatchData.Operator.AND, terms));
        }
        return new MatchResult(result, canBeTight);
    }

    /**
     * merge 2 predicate with AND between them, if they are the same we can intersect domains
     */
    private Optional<PredicateContext> tryMergeAndPredicates(PredicateContext existingPredicateContext, PredicateContext newPredicateContext)
    {
        Optional<PredicateContext> res = Optional.empty();
        if (existingPredicateContext.canMergeExpressions(newPredicateContext)) {
            NativeExpression expression1 = existingPredicateContext.getNativeExpression().get();
            NativeExpression expression2 = newPredicateContext.getNativeExpression().get();
            NativeExpression mergedNativeExpression = expression1.mergeAnd(expression2);
            WarpExpression warpExpression = new WarpCall(AND_FUNCTION_NAME.getName(),
                    List.of(existingPredicateContext.getExpression(), newPredicateContext.getExpression()),
                    BooleanType.BOOLEAN);
            res = Optional.of(new PredicateContext(
                    new WarpExpressionData(warpExpression,
                            existingPredicateContext.getColumnType(),
                            mergedNativeExpression.collectNulls(),
                            Optional.of(mergedNativeExpression),
                            existingPredicateContext.getWarpColumn())));
        }
        return res;
    }

    private MatchResult handleOrExpression(ClassifyArgs classifyArgs, WarpCall orExpression, Map<WarpExpression, PredicateContext> leaves)
    {
        List<MatchData> terms = new ArrayList<>();
        boolean canBeTight = true;
        for (WarpExpression warpExpression : orExpression.getArguments()) {
            checkArgument(warpExpression instanceof WarpCall, "warpExpression is not instance of WarpCall");
            String functionName = ((WarpCall) warpExpression).getFunctionName();
            if (functionName.equals(OR_FUNCTION_NAME.getName()) || functionName.equals(AND_FUNCTION_NAME.getName())) {
                MatchResult matchResult = handleLogicalFunction(classifyArgs, leaves, warpExpression);
                if (matchResult.matchData().isPresent()) {
                    terms.add(matchResult.matchData().get());
                    canBeTight = canBeTight && matchResult.canBeTight();
                }
                else {
                    terms = Collections.emptyList();
                    canBeTight = false;
                    break;
                }
            }
            else {
                PredicateContext predicateContext = leaves.get(warpExpression);
                if (predicateContext.getDomain().isNone()) {
                    terms.add(new NoneMatchData());
                    canBeTight = true;
                    continue;
                }
                WarpColumn warpColumn = predicateContext.getWarpColumn();
                MatchContext matchContext = runMatchers(classifyArgs, Map.of(warpColumn, predicateContext));

                checkArgument(matchContext.matchDataList().size() <= 1, "Too many matchData objects in the list");
                if (matchContext.matchDataList().isEmpty()) {
                    terms = Collections.emptyList();
                    canBeTight = false;
                    break;
                }
                else {
                    terms.add(matchContext.matchDataList().getFirst());
                }
            }
        }
        Optional<MatchData> result;
        checkArgument(terms.size() != 1, "can't contain 1 matchColumn under OR expression");
        if (terms.isEmpty()) {
            result = Optional.empty();
        }
        else {
            terms = terms.stream().filter(x -> !(x instanceof NoneMatchData)).collect(Collectors.toList());
            if (terms.isEmpty()) {
                //means all where none
                result = Optional.of(new NoneMatchData());
            }
            else if (terms.size() == 1) {
                result = Optional.of(terms.getFirst());
            }
            else {
                result = Optional.of(new LogicalMatchData(LogicalMatchData.Operator.OR, terms));
            }
        }
        return new MatchResult(result, canBeTight);
    }

    /**
     * @param remainingPredicateContext - A map of predicates with AND relation between them
     */
    private MatchContext runMatchers(ClassifyArgs classifyArgs, Map<WarpColumn, PredicateContext> remainingPredicateContext)
    {
        MatchContext matchContext = new MatchContext(Collections.emptyList(), remainingPredicateContext, true);
        if (!remainingPredicateContext.isEmpty()) {
            for (Matcher matcher : matchers) {
                matchContext = matcher.match(classifyArgs, matchContext);
                if (!matchContext.validRange()) {
                    break;
                }
            }
        }
        return matchContext;
    }

    @SuppressWarnings("unused")
    private record MatchResult(Optional<MatchData> matchData, boolean canBeTight) {}
}
