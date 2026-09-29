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
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A cast whose source and target base types form a pair in {@link SupportedCasts}.
 * <p>
 * The engine's {@code Cast.Kind} is not a component: it is a function of the source and target types alone
 * ({@code TypeCoercion#isTypeOnlyCoercion}), so code that rebuilds an engine cast derives it with
 * {@code IrExpressions.cast}.
 */
@JsonTypeName(Cast.NAME)
public record Cast(Expression expression, Type type)
        implements Expression
{
    static final String NAME = "Cast";
    static final int VERSION = 1;

    public Cast
    {
        requireNonNull(expression, "expression is null");
        requireNonNull(type, "type is null");
        checkArgument(
                SupportedCasts.contains(expression.type().getBaseName(), type.getBaseName()),
                "Cast: cast from %s to %s is not supported",
                expression.type().getBaseName(),
                type.getBaseName());
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
        return ImmutableList.of(expression);
    }

    @Override
    public Map<String, Integer> versionEntries()
    {
        String sourceBaseName = expression.type().getBaseName();
        String targetBaseName = type.getBaseName();
        return ImmutableMap.<String, Integer>builder()
                .putAll(Expression.super.versionEntries())
                .put(IrVersions.TYPE_LEAF, IrVersions.TYPE_LEAF_VERSION)
                .put(IrVersions.castKey(sourceBaseName, targetBaseName), SupportedCasts.semanticVersion(sourceBaseName, targetBaseName))
                .buildOrThrow();
    }
}
