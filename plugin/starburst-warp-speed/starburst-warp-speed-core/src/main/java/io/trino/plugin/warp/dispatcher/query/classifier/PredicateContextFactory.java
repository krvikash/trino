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
import com.google.inject.Inject;
import io.trino.plugin.warp.WarpSessionProperties;
import io.trino.plugin.warp.config.GlobalConfig;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.dispatcher.DispatcherTableHandle;
import io.trino.plugin.warp.dispatcher.model.RegularColumn;
import io.trino.plugin.warp.dispatcher.query.PredicateContext;
import io.trino.plugin.warp.expression.DomainExpression;
import io.trino.plugin.warp.expression.NativeExpression;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpExpressionData;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.gen.constants.FunctionType;
import io.trino.plugin.warp.gen.constants.PredicateType;
import io.trino.plugin.warp.util.DomainUtils;
import io.trino.plugin.warp.util.SimplifyResult;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.warp.type.TypeUtils.isWarmBasicSupported;
import static io.trino.spi.expression.StandardFunctions.AND_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class PredicateContextFactory
{
    private final GlobalConfig globalConfig;
    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    @Inject
    public PredicateContextFactory(GlobalConfig globalConfig, DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        this.globalConfig = globalConfig;
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
    }

    PredicateContextData create(
            ConnectorSession session,
            DynamicFilter dynamicFilter,
            DispatcherTableHandle dispatcherTableHandle)
    {
        Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> warpExpression = dispatcherTableHandle.getWarpExpression();
        if (!(dynamicFilter.getCurrentPredicate().isAll() || dynamicFilter.getCurrentPredicate().isNone())) {
            // in case we have dynamicFilter we can't use warpExpression. we probably need to intersect the expression as well with DF.
            warpExpression = Optional.empty();
        }
        TupleDomain<ColumnHandle> intersectTupleDomain = dispatcherTableHandle.getFullPredicate()
                .intersect(dynamicFilter.getCurrentPredicate());
        if (intersectTupleDomain.isNone()) {
            return new PredicateContextData(ImmutableMap.of(), WarpPrimitiveConstant.FALSE);
        }
        int predicateThreshold = WarpSessionProperties.getPredicateSimplifyThreshold(session, globalConfig);

        SimplifyResult<ColumnHandle> simplifyResult = DomainUtils.simplify(intersectTupleDomain, predicateThreshold);
        Set<RegularColumn> simplifiedColumns = Stream.concat(
                        dispatcherTableHandle.getSimplifiedColumns().simplifiedColumns().stream(),
                        simplifyResult.getSimplifiedColumns().stream().map(dispatcherProxiedConnectorTransformer::getWarpRegularColumn))
                .collect(Collectors.toSet());
        TupleDomain<ColumnHandle> tupleDomain = simplifyResult.getTupleDomain();
        return create(warpExpression, tupleDomain, simplifiedColumns);
    }

    private PredicateContextData create(
            Optional<io.trino.plugin.warp.expression.rewrite.WarpExpression> warpExpression,
            TupleDomain<ColumnHandle> tupleDomain,
            Set<RegularColumn> simplifiedColumns)
    {
        ImmutableMap.Builder<WarpExpression, PredicateContext> predicateContextMap = ImmutableMap.builder();
        warpExpression.ifPresent(expression -> {
            for (WarpExpressionData leaf : expression.warpExpressionDataLeaves()) {
                PredicateContext predicateContext = new PredicateContext(leaf);
                predicateContextMap.put(leaf.getExpression(), predicateContext);
            }
        });
        List<WarpExpression> domainExpressions = new ArrayList<>();
        tupleDomain.getDomains().ifPresent(columnHandleDomainMap -> columnHandleDomainMap.forEach((columnHandle, domain) -> {
            Type columnType = dispatcherProxiedConnectorTransformer.getColumnType(columnHandle);
            if (isWarmBasicSupported(columnType)) {
                WarpVariable warpVariable = new WarpVariable(columnHandle, domain.getType());
                RegularColumn warpColumn = dispatcherProxiedConnectorTransformer.getWarpRegularColumn(columnHandle);
                WarpExpression newWarpExpression = new DomainExpression(warpVariable, domain);
                boolean isSimplified = simplifiedColumns.contains(warpColumn);
                PredicateType predicateType = PredicateUtil.calcPredicateType(domain, columnType); // todo: move ClassifyArgs::getPredicateTypeFromCache to a global cache?
                NativeExpression nativeExpression = NativeExpression.builder()
                        .predicateType(predicateType)
                        .functionType(FunctionType.FUNCTION_TYPE_NONE)
                        .domain(domain)
                        .collectNulls(domain.isNullAllowed())
                        .build();
                WarpExpressionData warpExpressionData = new WarpExpressionData(
                        newWarpExpression,
                        columnType,
                        domain.isNullAllowed(),
                        Optional.of(nativeExpression),
                        warpColumn);
                PredicateContext predicateContext = new PredicateContext(warpExpressionData, isSimplified);
                domainExpressions.add(newWarpExpression);
                predicateContextMap.put(newWarpExpression, predicateContext);
            }
        }));
        WarpExpression rootExpression = WarpPrimitiveConstant.TRUE;

        if (warpExpression.isPresent() && !domainExpressions.isEmpty()) {
            WarpExpression warpRootExpression = warpExpression.get().rootExpression();
            if (warpRootExpression instanceof WarpCall warpCall && warpCall.getFunctionName().equals(AND_FUNCTION_NAME.getName())) {
                domainExpressions.addAll(warpRootExpression.getChildren());
            }
            else {
                domainExpressions.add(warpRootExpression);
            }
            rootExpression = new WarpCall(AND_FUNCTION_NAME.getName(), domainExpressions, BOOLEAN);
        }
        else if (warpExpression.isPresent()) {
            rootExpression = warpExpression.get().rootExpression();
        }
        else if (!domainExpressions.isEmpty()) {
            if (domainExpressions.size() == 1) {
                rootExpression = domainExpressions.getFirst();
            }
            else {
                rootExpression = new WarpCall(AND_FUNCTION_NAME.getName(), domainExpressions, BOOLEAN);
            }
        }
        return new PredicateContextData(predicateContextMap.buildOrThrow(), rootExpression);
    }
}
