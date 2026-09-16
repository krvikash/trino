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

import io.trino.sql.planner.exploratory.Memo;
import io.trino.sql.planner.exploratory.MemoOperation;

/**
 * Captures the facts relevant to applying a rule to a memo operation.
 * <p>
 * Rule application is cached by operation, rule, and application context. If the
 * context changes, the rule is considered eligible for re-application.
 * <p>
 * Implementations must be immutable and provide value-based {@link Object#equals(Object)}
 * and {@link Object#hashCode()} semantics. If the context omits facts relevant to the
 * rule, including by using {@link #EMPTY}, rule re-application may be missed when
 * those facts change.
 * <p>
 * In particular, operations are cached by value and {@link
 * MemoOperation#equals} ignores derived attributes. If matching or application depends on derived
 * attributes of the operation's group or another group, the relevant values should be obtained
 * through {@link Memo#getGroupAttributes(MemoOperation)} and included in the context.
 */
public interface RuleApplicationContext
{
    RuleApplicationContext EMPTY = EmptyRuleApplicationContext.INSTANCE;

    enum EmptyRuleApplicationContext
            implements RuleApplicationContext
    {
        INSTANCE
    }
}
