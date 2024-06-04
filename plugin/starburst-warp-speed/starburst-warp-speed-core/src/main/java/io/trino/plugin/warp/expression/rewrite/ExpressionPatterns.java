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

import io.trino.matching.Pattern;
import io.trino.matching.Property;
import io.trino.plugin.warp.expression.WarpCall;
import io.trino.plugin.warp.expression.WarpExpression;
import io.trino.spi.type.Type;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class ExpressionPatterns
{
    private ExpressionPatterns() {}

    public static Pattern<WarpCall> call()
    {
        return Pattern.typeOf(WarpCall.class);
    }

    public static Property<WarpCall, ?, Integer> argumentCount()
    {
        return Property.property("argumentCount", (warpCall) -> warpCall.getArguments().size());
    }

    public static Property<WarpCall, ?, String> functionName()
    {
        return Property.property("functionName", WarpCall::getFunctionName);
    }

    public static Property<WarpCall, ?, WarpExpression> argument(int argument)
    {
        checkArgument(0 <= argument, "Invalid argument index: %s", argument);
        return Property.optionalProperty(format("argument(%s)", argument), call -> {
            if (argument < call.getArguments().size()) {
                return Optional.of(call.getArguments().get(argument));
            }
            return Optional.empty();
        });
    }

    public static Property<WarpExpression, ?, Type> type()
    {
        return Property.property("type", WarpExpression::getType);
    }
}
