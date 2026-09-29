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
package io.starburst.materialization.ir;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static io.starburst.materialization.ir.IrMessages.describe;
import static java.util.Objects.requireNonNull;

/**
 * A call to a scalar function in {@link SupportedFunctions}. Comparisons and {@code NOT} are calls too, as in the
 * engine. The bound signature is the key a matcher re-resolves the function by; its argument types must agree with
 * the arguments' types.
 */
@JsonTypeName(Call.NAME)
public record Call(BoundSignature function, List<Expression> arguments)
        implements Expression
{
    static final String NAME = "Call";
    static final int VERSION = 1;

    public Call
    {
        requireNonNull(function, "function is null");
        arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
        checkArgument(SupportedFunctions.contains(function.getName()), "Call: function %s is not supported", function.getName());
        checkArgument(
                arguments.size() == function.getArity(),
                "Call: function %s expects %s arguments, got %s",
                function.getName(),
                function.getArity(),
                arguments.size());
        for (int i = 0; i < arguments.size(); i++) {
            Type argumentType = arguments.get(i).type();
            checkArgument(
                    argumentType.equals(function.getArgumentType(i)),
                    "Call: argument %s of function %s has type %s, expected %s",
                    i,
                    function.getName(),
                    describe(argumentType),
                    describe(function.getArgumentType(i)));
        }
    }

    @Override
    public Type type()
    {
        return function.getReturnType();
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public int version()
    {
        return VERSION;
    }

    @Override
    public List<Expression> children()
    {
        return arguments;
    }

    @Override
    public Map<String, Integer> versionEntries()
    {
        return ImmutableMap.<String, Integer>builder()
                .putAll(Expression.super.versionEntries())
                .put(IrVersions.BOUND_SIGNATURE_LEAF, IrVersions.BOUND_SIGNATURE_LEAF_VERSION)
                .put(IrVersions.functionKey(function.getName()), SupportedFunctions.semanticVersion(function.getName()))
                .buildOrThrow();
    }
}
