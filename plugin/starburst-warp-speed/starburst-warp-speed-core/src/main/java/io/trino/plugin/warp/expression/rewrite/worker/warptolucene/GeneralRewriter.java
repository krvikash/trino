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
package io.trino.plugin.warp.expression.rewrite.worker.warptolucene;

import io.airlift.slice.Slice;
import io.trino.matching.Pattern;
import io.trino.plugin.warp.dispatcher.DispatcherProxiedConnectorTransformer;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.WarpVariable;
import io.trino.plugin.warp.expression.rewrite.ExpressionPatterns;
import io.trino.spi.predicate.Range;
import io.trino.spi.type.Type;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.warp.storage.lucene.LuceneQueryUtils.createContainsQuery;
import static io.trino.plugin.warp.storage.lucene.LuceneQueryUtils.createLikeQuery;
import static io.trino.plugin.warp.storage.lucene.LuceneQueryUtils.createPrefixQuery;
import static io.trino.plugin.warp.storage.lucene.LuceneQueryUtils.createRangeQuery;

class GeneralRewriter
        implements ExpressionRewriter<WarpCall>
{
    private static final Pattern<WarpCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(2))
            .with(argument(0).matching(x -> x instanceof WarpVariable))
            .with(argument(1).matching(x -> x instanceof WarpSliceConstant));

    private final DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer;

    GeneralRewriter(DispatcherProxiedConnectorTransformer dispatcherProxiedConnectorTransformer)
    {
        this.dispatcherProxiedConnectorTransformer = dispatcherProxiedConnectorTransformer;
    }

    @Override
    public Pattern<WarpCall> getPattern()
    {
        return PATTERN;
    }

    boolean handleLike(WarpExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createLikeQuery(value));
    }

    public boolean handleContains(WarpExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createContainsQuery(value));
    }

    boolean handleStartsWith(WarpExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createPrefixQuery(value));
    }

    boolean handleNotEqual(WarpExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> {
            Query lessThan = createRangeQuery(Range.lessThan(type, value));
            Query greaterThan = createRangeQuery(Range.greaterThan(type, value));
            BooleanQuery.Builder innerQueryBuilder = new BooleanQuery.Builder();
            innerQueryBuilder.add(lessThan, BooleanClause.Occur.SHOULD);
            innerQueryBuilder.add(greaterThan, BooleanClause.Occur.SHOULD);
            return innerQueryBuilder.build();
        });
    }

    boolean handleEqual(WarpExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.equal(type, value)));
    }

    boolean handleLessThan(WarpExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.lessThan(type, value)));
    }

    boolean handleLessThanOrEqual(WarpExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.lessThanOrEqual(type, value)));
    }

    boolean greateThan(WarpExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.greaterThan(type, value)));
    }

    boolean greatThanOrEqual(WarpExpression expression, LuceneRewriteContext context)
    {
        return rewrite(expression, context, (value, type) -> createRangeQuery(Range.greaterThanOrEqual(type, value)));
    }

    private boolean rewrite(WarpExpression expression, LuceneRewriteContext context, BiFunction<Slice, Type, Query> queryFunction)
    {
        Type type = getType((WarpVariable) expression.getChildren().get(0));
        WarpConstant varadaConstant = (WarpConstant) expression.getChildren().get(1);
        checkArgument(varadaConstant instanceof WarpSliceConstant, "%s is not VaradaSliceConstant", varadaConstant);
        Slice value = ((Slice) varadaConstant.getValue());
        Query query = queryFunction.apply(value, type);
        context.queryBuilder().add(query, context.occur());
        return true;
    }

    private Type getType(WarpVariable varadaVariable)
    {
        return dispatcherProxiedConnectorTransformer.getColumnType(varadaVariable.getColumnHandle());
    }
}
