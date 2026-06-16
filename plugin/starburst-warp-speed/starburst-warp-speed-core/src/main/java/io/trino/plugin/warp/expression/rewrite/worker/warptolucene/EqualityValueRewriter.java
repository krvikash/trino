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
import io.airlift.slice.Slices;
import io.trino.matching.Pattern;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpConstant;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpPrimitiveConstant;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.util.SliceUtils;
import io.trino.spi.type.BooleanType;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argumentCount;
import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.functionName;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.LTRIM;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.RTRIM;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.SPLIT_PART;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.STRPOS;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.SUBSTR;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.SUBSTRING;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.TRIM;
import static io.trino.plugin.warp.storage.lucene.LuceneQueryUtils.createLikeQuery;
import static io.trino.spi.expression.StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME;

public class EqualityValueRewriter
        implements ExpressionRewriter<WarpCall>
{
    private static final Pattern<WarpCall> PATTERN = ExpressionPatterns.call()
            .with(argumentCount().equalTo(2))
            .with(functionName().matching(x -> x.equals(EQUAL_OPERATOR_FUNCTION_NAME.getName())))
            .with(argument(0).matching(x -> x instanceof WarpCall warpCall &&
                    (warpCall.getFunctionName().equals(SUBSTRING.getName()) ||
                            warpCall.getFunctionName().equals(SPLIT_PART.getName()) ||
                            warpCall.getFunctionName().equals(TRIM.getName()) ||
                            warpCall.getFunctionName().equals(LTRIM.getName()) ||
                            warpCall.getFunctionName().equals(RTRIM.getName()) ||
                            warpCall.getFunctionName().equals(SUBSTR.getName()) ||
                            warpCall.getFunctionName().equals(STRPOS.getName()))))
            .with(argument(1).matching(x -> x instanceof WarpConstant && !x.getType().equals(BooleanType.BOOLEAN)));

    public EqualityValueRewriter() {}

    @Override
    public Pattern<WarpCall> getPattern()
    {
        return PATTERN;
    }

    public boolean handleEqual(WarpExpression expression, LuceneRewriteContext context)
    {
        WarpCall warpCall = (WarpCall) expression.getChildren().get(0);
        WarpConstant warpConstant;
        if (warpCall.getFunctionName().equals(STRPOS.getName())) {
            WarpExpression positionValue = expression.getChildren().get(1);
            if (positionValue instanceof WarpPrimitiveConstant warpPrimitiveConstant) {
                if (Integer.valueOf(0).equals(warpPrimitiveConstant.getValue())) {
                    // See https://stackoverflow.com/a/16091066, =false->false, =true->true, !=true->false, !=false->true
                    context.queryBuilder().add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.SHOULD);
                    context = createContext(context, BooleanClause.Occur.MUST_NOT);
                }
                warpConstant = (WarpConstant) warpCall.getChildren().get(1);
            }
            else {
                return false;
            }
        }
        else {
            warpConstant = (WarpConstant) expression.getChildren().get(1);
        }
        checkArgument(warpConstant instanceof WarpSliceConstant, "%s is not WarpSliceConstant", warpConstant);
        Slice sliceValue = ((Slice) warpConstant.getValue());
        String val = SliceUtils.serializeSlice(sliceValue);
        Slice likeSlice = Slices.utf8Slice("%" + val + "%");
        Query likeQuery = createLikeQuery(likeSlice);
        context.queryBuilder().add(likeQuery, context.occur());
        return true;
    }
}
