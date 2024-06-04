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
package io.trino.plugin.warp.expression.rewrite;

import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.MoreCollectors;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.trino.matching.Capture;
import io.trino.matching.Match;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.config.NativeConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.model.TransformedColumn;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.TransformFunction;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions;
import io.trino.plugin.warp.expression.rewrite.coordinator.warptonative.NativeExpressionRulesHandler;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.gen.stats.PushdownPredicatesStats;
import io.trino.plugin.warp.metrics.MetricsManager;
import io.trino.plugin.warp.type.TypeUtils;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static io.trino.matching.Capture.newCapture;
import static io.trino.plugin.warp.type.TypeUtils.isLongTimeWithTimeZoneType;
import static io.trino.plugin.warp.type.TypeUtils.isLongTimestampType;
import static io.trino.plugin.warp.type.TypeUtils.isLongTimestampTypeWithTimeZoneType;
import static io.trino.plugin.warp.type.TypeUtils.isWarmBasicSupported;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IS_NULL_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static java.util.Objects.requireNonNull;

@Singleton
public class ExpressionService
{
    public static final String PUSHDOWN_PREDICATES_STAT_GROUP = "pushdown-predicates";
    private static final int MAX_TREE_LEVEL = 4;

    private static final Logger logger = Logger.get(ExpressionService.class);

    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;
    private final SupportedFunctions supportedFunctions;
    private final NativeExpressionRulesHandler nativeExpressionRulesHandler;
    private final GlobalConfig globalConfig;
    private final NativeConfig nativeConfig;
    private final PushdownPredicatesStats pushdownPredicatesStats;

    @Inject
    public ExpressionService(
            DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer,
            SupportedFunctions supportedFunctions,
            GlobalConfig globalConfig,
            NativeConfig nativeConfig,
            MetricsManager metricsManager,
            NativeExpressionRulesHandler nativeExpressionRulesHandler)
    {
        this.globalConfig = requireNonNull(globalConfig);
        this.nativeConfig = requireNonNull(nativeConfig);
        this.dispatcherProxiedConnectorTransformer = requireNonNull(dispatcherProxiedConnectorTransformer);
        this.supportedFunctions = requireNonNull(supportedFunctions);
        this.pushdownPredicatesStats = metricsManager.registerMetric(PushdownPredicatesStats.create(PUSHDOWN_PREDICATES_STAT_GROUP));
        this.nativeExpressionRulesHandler = requireNonNull(nativeExpressionRulesHandler);
    }

    public Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> convertToWarpExpression(ConnectorSession session,
                                                            ConnectorExpression expression,
                                                            Map<String, ColumnHandle> assignments,
                                                            Map<String, Long> customStats)
    {
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> res;
        try {
            if (!WarpSessionProperties.getEnableOrPushdown(session)) {
                return Optional.empty();
            }
            Set<String> unsupportedFunctions = WarpSessionProperties.getUnsupportedFunctions(session, globalConfig);
            if (unsupportedFunctions.size() == 1 &&
                    unsupportedFunctions.stream().collect(MoreCollectors.onlyElement()).equals("*")) {
                return Optional.empty();
            }
            Optional<WarpExpression> warpExpressionOpt = convertToWarpExpression(session,
                    expression,
                    assignments,
                    unsupportedFunctions,
                    customStats);
            if (warpExpressionOpt.isPresent()) {
                ImmutableSetMultimap.Builder<RegularColumn, WarpExpressionData> outWarpExpressionDataLeaves = ImmutableSetMultimap.builder();
                Set<String> unsupportedNativeFunctions = WarpSessionProperties.getUnsupportedNativeFunctions(session, nativeConfig);

                boolean validExpression = convertToFlatWarpExpressionDataList(warpExpressionOpt.get(), outWarpExpressionDataLeaves, unsupportedNativeFunctions, customStats, 0);
                List<WarpExpressionData> warpExpressionDataLeaves = new ArrayList<>(outWarpExpressionDataLeaves.build().values());

                if (warpExpressionDataLeaves.isEmpty() ||
                        !validExpression) {
                    res = Optional.empty();
                }
                else {
                    res = Optional.of(new io.trino.plugin.warp.expression.rewrite.WarpExpression(warpExpressionOpt.get(), warpExpressionDataLeaves));
                }
            }
            else {
                res = Optional.empty();
            }
        }
        catch (Exception e) {
            logger.warn("failed to convert expression to warpExpression. expression=%s, error=%s", expression, e.getMessage());
            pushdownPredicatesStats.incfailed_rewrite_expression();
            res = Optional.empty();
        }
        return res;
    }

    private boolean convertToFlatWarpExpressionDataList(WarpExpression warpExpression,
                                                          ImmutableSetMultimap.Builder<RegularColumn, WarpExpressionData> outWarpExpressionDataLeaves,
                                                          Set<String> unsupportedNativeFunctions,
                                                          Map<String, Long> customStats,
                                                          int treeLevel)
    {
        if (warpExpression instanceof WarpCall warpCall) {
            if (warpCall.getFunctionName().equals(OR_FUNCTION_NAME.getName()) ||
                    warpCall.getFunctionName().equals(AND_FUNCTION_NAME.getName())) {
                if (treeLevel == MAX_TREE_LEVEL) {
                    pushdownPredicatesStats.incunsupported_expression_depth();
                    return false;
                }
                treeLevel++;
                for (WarpExpression expression : warpCall.getArguments()) {
                    if (!convertToFlatWarpExpressionDataList(expression, outWarpExpressionDataLeaves, unsupportedNativeFunctions, customStats, treeLevel)) {
                        return false;
                    }
                }
            }
            else {
                Optional<ColumnHandle> columnHandleOptional = getColumnHandle(warpExpression);
                if (columnHandleOptional.isEmpty()) {
                    return false;
                }

                Type columnType = dispatcherProxiedConnectorTransformer.getColumnType(columnHandleOptional.get());
                RegularColumn warpColumn = dispatcherProxiedConnectorTransformer.getWarpRegularColumn(columnHandleOptional.get());

                Optional<NativeExpression> nativeExpressionOptional;
                if (isNullExpression(warpExpression)) {
                    nativeExpressionOptional = Optional.of(new NativeExpression(PredicateType.PREDICATE_TYPE_VALUES,
                            FunctionType.FUNCTION_TYPE_NONE,
                            Domain.onlyNull(columnType),
                            true,
                            true,
                            Collections.emptyList(),
                            TransformFunction.NONE));
                }
                else {
                    nativeExpressionOptional = nativeExpressionRulesHandler.rewrite(warpExpression, columnType, unsupportedNativeFunctions, customStats);
                }

                Optional<RegularColumn> column;
                if (nativeExpressionOptional.isPresent() &&
                        !Objects.equals(nativeExpressionOptional.get().transformFunction(), TransformFunction.NONE)) {
                    column = getTransformedColumn(warpColumn, warpExpression, nativeExpressionOptional.get().transformFunction());
                }
                else {
                    column = Optional.of(warpColumn);
                }

                if (column.isPresent() && isSupportedColumnType(columnType)) {
                    WarpExpressionData warpExpressionData = new WarpExpressionData(warpExpression,
                            columnType,
                            nativeExpressionOptional.isPresent() && nativeExpressionOptional.get().collectNulls(),
                            nativeExpressionOptional,
                            column.get());
                    outWarpExpressionDataLeaves.put(warpColumn, warpExpressionData);
                }
            }
        }
        return true;
    }

    private Optional<WarpExpression> convertToWarpExpression(ConnectorSession session,
            ConnectorExpression expression,
            Map<String, ColumnHandle> assignments,
            Set<String> unsupportedFunctions,
            Map<String, Long> customStats)
    {
        Optional<WarpExpression> res;
        if (expression instanceof Variable variable && isSupportedColumnType(variable.getType())) {
            ColumnHandle columnHandle = assignments.get(variable.getName());
            Type type = variable.getType();
            res = Optional.of(new WarpVariable(columnHandle, type));
        }
        else if (expression instanceof Call call) {
            FunctionName functionName = call.getFunctionName();
            if (unsupportedFunctions.contains(functionName.getName())) {
                logger.debug("%s function is listed in unsupportedFunctions list. skip", functionName.getName());
                res = Optional.empty();
            }
            else {
                Set<ConnectorExpressionRule<Call, WarpExpression>> rule = supportedFunctions.getRule(functionName);
                if (rule.isEmpty()) {
                    res = Optional.empty();
                    pushdownPredicatesStats.incunsupported_functions();
                    customStats.compute("unsupported_functions", (key, value) -> value == null ? 1L : value + 1);
                }
                else {
                    ConnectorExpressionRule.RewriteContext<WarpExpression> context = createContext(assignments, session, unsupportedFunctions, customStats);
                    res = rewrite(rule, expression, context, customStats);
                }
            }
        }
        else if (expression instanceof Constant constant) {
            if (constant.getValue() instanceof Slice) {
                // value of the constant must be typed so a valid serializer/deserializer will be used
                res = Optional.of(new WarpSliceConstant((Slice) constant.getValue(), constant.getType()));
            }
            else {
                // workaround: cannot use instanceof since JsonPathType is not part of the trino-spi module (different classloader)
                if (constant.getType().getClass().getName().endsWith("JsonPathType")) {
                    // no need to convert the JsonPath object, it's enough to convert only the pattern
                    // use varchar for the type since JsonPath is not part of the trino-spi module
                    res = Optional.of(new WarpPrimitiveConstant(constant.getValue().toString(), VarcharType.VARCHAR));
                }
                else {
                    res = Optional.of(new WarpPrimitiveConstant(constant.getValue(), constant.getType()));
                }
            }
        }
        else {
            res = Optional.empty();
        }
        return res;
    }

    private ConnectorExpressionRule.RewriteContext<WarpExpression> createContext(Map<String, ColumnHandle> assignments,
            ConnectorSession session,
            Set<String> unsupportedFunctions,
            Map<String, Long> customStats)
    {
        return new ConnectorExpressionRule.RewriteContext<>()
        {
            @Override
            public Map<String, ColumnHandle> getAssignments()
            {
                return assignments;
            }

            @Override
            public ConnectorSession getSession()
            {
                return session;
            }

            @Override
            public Optional<WarpExpression> defaultRewrite(ConnectorExpression expression)
            {
                return convertToWarpExpression(session, expression, assignments, unsupportedFunctions, customStats);
            }
        };
    }

    private Optional<WarpExpression> rewrite(
            Set<ConnectorExpressionRule<Call, WarpExpression>> rules,
            ConnectorExpression expression,
            ConnectorExpressionRule.RewriteContext<WarpExpression> context,
            Map<String, Long> customStats)
    {
        Capture<Call> expressionCapture = newCapture();
        Optional<WarpExpression> res = Optional.empty();
        boolean anyMatch = false;
        for (ConnectorExpressionRule<Call, WarpExpression> rule : rules) {
            Pattern<? extends ConnectorExpression> pattern = rule.getPattern().capturedAs(expressionCapture);
            Optional<Match> matches = pattern.match(expression, context).findFirst();
            if (matches.isPresent()) {
                anyMatch = true;
                Match match = matches.get();
                Call capturedExpression = match.capture(expressionCapture);
                verify(Objects.equals(capturedExpression, expression));
                Optional<WarpExpression> rewritten = rule.rewrite(capturedExpression, match.captures(), context);
                if (rewritten.isPresent()) {
                    res = rewritten;
                    break;
                }
            }
        }
        if (!anyMatch) {
            customStats.compute("unsupported_functions", (key, value) -> value == null ? 1L : value + 1);
            pushdownPredicatesStats.incunsupported_functions();
        }
        return res;
    }

    /**
     * get columnHandle from leaf expression, if a leaf contains 2 column we drop that expression since it not supported
     */
    public static Optional<ColumnHandle> getColumnHandle(WarpExpression warpExpression)
    {
        if (warpExpression instanceof WarpVariable variable) {
            return Optional.of(variable.getColumnHandle());
        }
        if (warpExpression instanceof WarpCall warpCall) {
            Optional<ColumnHandle> res = Optional.empty();
            for (WarpExpression child : warpCall.getArguments()) {
                Optional<ColumnHandle> columnHandle = getColumnHandle(child);
                if (columnHandle.isPresent()) {
                    if (res.isPresent()) {
                        // More than one predicate appears in the expression, not supported
                        return Optional.empty();
                    }
                    else {
                        res = columnHandle;
                    }
                }
            }
            return res;
        }
        return Optional.empty();
    }

    private static boolean isSupportedColumnType(Type columnType)
    {
        boolean res = true;
        if (TypeUtils.isLongDecimalType(columnType)) {
            //todo can't serialize LongDecimalType since Int128 is not serializable
            res = false;
        }
        else if (isLongTimestampType(columnType) || isLongTimestampTypeWithTimeZoneType(columnType) || isLongTimeWithTimeZoneType(columnType)) {
            res = false;
        }
        else if (columnType instanceof MapType mapType) {
            return isSupportedColumnType(mapType.getKeyType()) && isSupportedColumnType(mapType.getValueType()) && isWarmBasicSupported(mapType.getValueType());
        }
        return res;
    }

    private Optional<RegularColumn> getTransformedColumn(RegularColumn regularColumn, WarpExpression warpExpression, TransformFunction transformFunction)
    {
        Optional<RegularColumn> res = Optional.empty();

        if (warpExpression instanceof WarpCall warpCall) {
            String functionName = warpCall.getFunctionName();

            if (supportedFunctions.getComparableStandardFunctions().contains(functionName)) {
                for (WarpExpression child : warpCall.getArguments()) {
                    if (child instanceof WarpCall) {
                        TransformedColumn transformedColumn = new TransformedColumn(regularColumn.getName(), regularColumn.getColumnId(), transformFunction);
                        res = Optional.of(transformedColumn);
                    }
                }
            }
            else if (functionName.equals(IN_PREDICATE_FUNCTION_NAME.getName()) &&
                    warpCall.getArguments().get(0) instanceof WarpCall) {
                TransformedColumn transformedColumn = new TransformedColumn(regularColumn.getName(), regularColumn.getColumnId(), transformFunction);
                res = Optional.of(transformedColumn);
            }
        }
        return res;
    }

    private boolean isNullExpression(WarpExpression warpExpression)
    {
        return warpExpression instanceof WarpCall warpCall && warpCall.getFunctionName().equals(IS_NULL_FUNCTION_NAME.getName());
    }
}
