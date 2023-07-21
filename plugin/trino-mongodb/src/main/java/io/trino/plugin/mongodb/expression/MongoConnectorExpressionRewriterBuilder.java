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

import com.google.common.collect.ImmutableSet;
import io.trino.plugin.base.expression.ConnectorExpressionRewriter;
import io.trino.plugin.base.expression.ConnectorExpressionRule;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;

import static io.trino.spi.type.StandardTypes.JSON;

public class MongoConnectorExpressionRewriterBuilder
{
    public static MongoConnectorExpressionRewriterBuilder newBuilder()
    {
        return new MongoConnectorExpressionRewriterBuilder();
    }

    private final ImmutableSet.Builder<ConnectorExpressionRule<?, MongoExpression>> rules = ImmutableSet.builder();

    private MongoConnectorExpressionRewriterBuilder() {}

    public MongoConnectorExpressionRewriterBuilder addRules(TypeManager typeManager)
    {
        add(new RewriteVariable());
        add(new RewriteBooleanConstant());
        add(new RewriteVarcharConstant());
        add(new RewriteVarbinaryConstant());
        add(new RewriteExactNumericConstant());
        add(new RewriteObjectIdConstant());
        add(new RewriteDateTimeConstant());
        add(new RewriteAnd());
        add(new RewriteOr());
        add(new RewriteComparison());
        add(new RewriteIn());
        add(new RewriteNot());
        add(new RewriteIsNull());
        add(new RewriteJsonConstant(typeManager.getType(new TypeSignature(JSON))));
        add(new RewriteJsonPath(typeManager.getType(new TypeSignature("JsonPath"))));
        add(new RewriteJsonExtractScalar());
        add(new RewriteFromIso8601Date());
        add(new RewriteFromIso8601Timestamp());

        return this;
    }

    public MongoConnectorExpressionRewriterBuilder add(ConnectorExpressionRule<?, MongoExpression> rule)
    {
        rules.add(rule);
        return this;
    }

    public ConnectorExpressionRewriter<MongoExpression> build()
    {
        return new ConnectorExpressionRewriter<>(rules.build());
    }
}
