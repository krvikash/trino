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

import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.sql.planner.exploratory.Memo;
import io.trino.sql.planner.exploratory.MemoOperation;

public interface OperationRule
{
    Pattern<MemoOperation> pattern();

    Result<NewMemoOperation> apply(MemoOperation operation, Captures captures, Context context);

    /**
     * Returns the application context relevant to this rule for the given memo operation.
     * <p>
     * Rule application is cached by operation, rule, and application context. If the
     * returned context changes, the rule becomes eligible for re-application.
     * <p>
     * The default implementation returns {@link RuleApplicationContext#EMPTY}, which
     * means the rule is applied at most once per memo operation. If that context is
     * insufficient for the rule, re-application may be missed when relevant conditions change.
     */
    default RuleApplicationContext getApplicationContext(MemoOperation operation, Memo memo)
    {
        return RuleApplicationContext.EMPTY;
    }

    /**
     * Indicates whether the source MemoOperation can be removed from Memo.
     * By default, rules do not replace the source operation.
     * It is only correct to replace the source operation if the change is monotonic,
     * and removing the original operation will not block out other optimizations.
     */
    default boolean canReplaceSource()
    {
        return false;
    }
}
