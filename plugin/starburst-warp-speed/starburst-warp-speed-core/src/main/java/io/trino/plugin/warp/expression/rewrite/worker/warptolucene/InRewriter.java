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
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.plugin.warp.expression.WarpSliceConstant;
import io.trino.plugin.warp.expression.rewrite.ExpressionPatterns;
import io.trino.plugin.warp.util.SliceUtils;
import org.apache.lucene.search.Query;

import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.warp.expression.rewrite.ExpressionPatterns.argument;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.LTRIM;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.RTRIM;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.SPLIT_PART;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.SUBSTR;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.SUBSTRING;
import static io.trino.plugin.warp.expression.rewrite.coordinator.connectortowarp.SupportedFunctions.TRIM;
import static io.trino.plugin.warp.storage.lucene.LuceneQueryUtils.createOrOfLikesQuery;
import static io.trino.spi.expression.StandardFunctions.ARRAY_CONSTRUCTOR_FUNCTION_NAME;
import static io.trino.spi.expression.StandardFunctions.IN_PREDICATE_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;

public class InRewriter
        implements ExpressionRewriter<WarpCall>

{
    private static final Pattern<WarpCall> PATTERN = ExpressionPatterns.call()
            .with(ExpressionPatterns.functionName().equalTo(IN_PREDICATE_FUNCTION_NAME.getName()))
            .with(ExpressionPatterns.type().equalTo(BOOLEAN))
            .with(argument(0).matching(x -> x instanceof WarpCall warpCall &&
                    (warpCall.getFunctionName().equals(SUBSTRING.getName()) ||
                            warpCall.getFunctionName().equals(SPLIT_PART.getName()) ||
                            warpCall.getFunctionName().equals(TRIM.getName()) ||
                            warpCall.getFunctionName().equals(LTRIM.getName()) ||
                            warpCall.getFunctionName().equals(RTRIM.getName()) ||
                            warpCall.getFunctionName().equals(SUBSTR.getName()))))
            .with(ExpressionPatterns.argumentCount().equalTo(2))
            .with(ExpressionPatterns.argument(1).matching(ExpressionPatterns.call().with(ExpressionPatterns.functionName().equalTo(ARRAY_CONSTRUCTOR_FUNCTION_NAME.getName()))));

    @Override
    public Pattern<WarpCall> getPattern()
    {
        return PATTERN;
    }

    boolean handleIn(WarpExpression expression, LuceneRewriteContext context)
    {
        boolean res = false;
        List<Slice> likeValues = new ArrayList<>();
        for (WarpExpression valueExpression : expression.getChildren().get(1).getChildren()) {
            if (valueExpression instanceof WarpSliceConstant warpSliceConstant) {
                Slice sliceValue = warpSliceConstant.getValue();
                String val = SliceUtils.serializeSlice(sliceValue);
                likeValues.add(Slices.utf8Slice("%" + val + "%"));
            }
            else {
                likeValues.clear();
                break;
            }
        }
        if (!likeValues.isEmpty()) {
            Query listOfLikeQuery = createOrOfLikesQuery(likeValues);
            context.queryBuilder().add(listOfLikeQuery, context.occur());
            res = true;
        }
        return res;
    }
}
