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
package io.trino.sql.planner.exploratory.rule;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.newir.Attributes;
import io.trino.sql.planner.exploratory.Memo;
import io.trino.sql.planner.exploratory.MemoOperation;
import io.trino.sql.planner.exploratory.MemoOperation.GroupChild;
import io.trino.sql.planner.exploratory.MemoOperation.ParameterChild;

import java.util.List;

import static io.trino.matching.Pattern.nonEmpty;
import static io.trino.sql.dialect.ir.IrDialect.DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
import static io.trino.sql.planner.exploratory.AttributeUtils.updateOperationAttributes;
import static io.trino.sql.planner.exploratory.rule.NewMemoOperation.operationLike;
import static io.trino.sql.planner.exploratory.rule.Patterns.children;
import static io.trino.sql.planner.exploratory.rule.Patterns.operation;
import static java.util.Objects.requireNonNull;

public class UpdateOperationAttributes
        implements OperationRule
{
    private static final Pattern<MemoOperation> PATTERN = operation().with(nonEmpty(children()));

    @Override
    public Pattern<MemoOperation> pattern()
    {
        return PATTERN;
    }

    @Override
    public boolean canReplaceSource()
    {
        return true;
    }

    @Override
    public RuleApplicationContext getApplicationContext(MemoOperation operation, Memo memo)
    {
        return new UpdateOperationAttributesContext(
                operation.attributes(),
                childAttributes(operation, memo.getRuleContext()));
    }

    @Override
    public Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context)
    {
        List<Attributes> childAttributes = childAttributes(operation, context);

        Attributes derivedAttributes = operation.deriveAttributes(childAttributes, context.getDialectRegistry());
        Attributes updatedAttributes = updateOperationAttributes(
                operation.attributes(),
                operation.inherentAttributes(),
                derivedAttributes,
                context.getDialectRegistry());

        if (updatedAttributes.equals(operation.attributes())) {
            return Result.unchanged();
        }

        return Result.of(operationLike(operation)
                .withAttributes(updatedAttributes));
    }

    private static List<Attributes> childAttributes(MemoOperation operation, Context context)
    {
        return operation.children().stream()
                .map(child ->
                        switch (child) {
                            case ParameterChild _ -> DEFAULT_BLOCK_PARAMETER_ATTRIBUTES;
                            case GroupChild groupChild -> context.getGroup(groupChild.groupId()).attributes();
                        })
                .toList();
    }

    private record UpdateOperationAttributesContext(
            Attributes operationAttributes,
            List<Attributes> childAttributes)
            implements RuleApplicationContext
    {
        private UpdateOperationAttributesContext
        {
            requireNonNull(operationAttributes, "operationAttributes is null");
            childAttributes = ImmutableList.copyOf(requireNonNull(childAttributes, "childAttributes is null"));
        }
    }
}
