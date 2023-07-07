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
package io.trino.plugin.mongodb.expression;

import io.airlift.slice.Slice;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.expression.Constant;
import io.trino.spi.type.Type;

import java.util.Optional;

import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.constant;
import static io.trino.plugin.base.expression.ConnectorExpressionPatterns.type;

public class RewriteJsonConstant
        implements ConnectorExpressionRule<Constant, MongoExpression>
{
    private final Pattern<Constant> pattern;

    public RewriteJsonConstant(Type jsonType)
    {
        this.pattern = constant().with(type().matching(jsonType.getClass()::isInstance));
    }

    @Override
    public Pattern<Constant> getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<MongoExpression> rewrite(Constant constant, Captures captures, RewriteContext<MongoExpression> context)
    {
        if (constant.getValue() == null) {
            return Optional.empty();
        }
        Slice slice = (Slice) constant.getValue();
        if (slice == null) {
            return Optional.empty();
        }
        return Optional.of(new MongoExpression(slice.toStringUtf8()));
    }
}
