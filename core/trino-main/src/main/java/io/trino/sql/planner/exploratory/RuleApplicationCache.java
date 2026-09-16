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
package io.trino.sql.planner.exploratory;

import io.trino.sql.planner.exploratory.rule.OperationRule;
import io.trino.sql.planner.exploratory.rule.RuleApplicationContext;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import static io.trino.sql.planner.exploratory.rule.RuleApplicationContext.EMPTY;

// TODO limit the size of the cache (ENG-20614)
//  - go to cache AFTER a rule was matched to the operation (before apply()),
//    not before the match attempt
//  - do not match every rule to every operation. Pre-filter by operation type
//    where applicable
//  - keep entries for removed operations, so if an operation re-appears we do
//    not repeat already processed rule applications
//  - introduce rule indexing and cache by index, not by the OperationRule
//  - key rules by identity, not by the OperationRule instance: two instances of the
//    same rule must be treated as the same rule (achieved by the rule indexing above)
//  - cache particular matches, not just the fact of rule application. It will
//    allow us to skip re-applying the rule for already processed matches during
//    re-application.

/**
 * Caches previously seen rule application contexts for memo operations.
 * <p>
 * Semantically, the cache answers whether a given {@code (operation, rule, context)}
 * triple was already seen. A new triple means the rule should be applied; a repeated
 * triple means the rule can be skipped.
 * <p>
 * To reduce the common-case memory overhead, pairs that have only ever seen
 * {@link RuleApplicationContext#EMPTY} are stored in a compact dedicated structure.
 */
final class RuleApplicationCache
{
    /**
     * Operations are keyed by value ({@link MemoOperation#equals(Object)}), which intentionally ignores
     * derived attributes. If a rule's application depends on derived attributes, the relevant
     * values must be captured in the {@link RuleApplicationContext}, not inferred from the
     * operation key. Rules should obtain derived attributes through {@link
     * Memo#getGroupAttributes(MemoOperation)}, because group attributes reflect more up-to-date
     * cumulative knowledge than attributes stored on the operation.
     */
    private final Map<MemoOperation, Set<OperationRule>> seenEmptyContexts = new LinkedHashMap<>();
    private final Map<MemoOperation, Map<OperationRule, Set<RuleApplicationContext>>> seenNonEmptyContexts = new LinkedHashMap<>();

    /**
     * Records the given rule application context and returns whether it was new.
     * <p>
     * Returning {@code true} means the cache had not seen this exact
     * {@code (operation, rule, context)} triple before, so the rule should be applied.
     * Returning {@code false} means the same triple was already recorded and can be skipped.
     */
    public boolean markIfNew(MemoOperation operation, OperationRule rule, RuleApplicationContext context)
    {
        if (context == EMPTY) {
            return markEmptyIfNew(operation, rule);
        }

        return markNonEmptyIfNew(operation, rule, context);
    }

    private boolean markEmptyIfNew(MemoOperation operation, OperationRule rule)
    {
        Set<RuleApplicationContext> seenContexts = getNonEmptyContexts(operation, rule);
        if (seenContexts != null) {
            return seenContexts.add(EMPTY);
        }

        return seenEmptyContexts
                .computeIfAbsent(operation, _ -> new LinkedHashSet<>())
                .add(rule);
    }

    private boolean markNonEmptyIfNew(MemoOperation operation, OperationRule rule, RuleApplicationContext context)
    {
        Set<RuleApplicationContext> seenContexts = getNonEmptyContexts(operation, rule);
        if (seenContexts != null) {
            return seenContexts.add(context);
        }

        Set<OperationRule> seenEmptyRules = seenEmptyContexts.get(operation);
        boolean seenEmpty = seenEmptyRules != null && seenEmptyRules.remove(rule);
        if (seenEmpty) {
            removeEmptyOperationEntryIfUnused(operation);
        }

        seenContexts = seenNonEmptyContexts
                .computeIfAbsent(operation, _ -> new LinkedHashMap<>())
                .computeIfAbsent(rule, _ -> new LinkedHashSet<>());
        if (seenEmpty) {
            seenContexts.add(EMPTY);
        }
        return seenContexts.add(context);
    }

    private Set<RuleApplicationContext> getNonEmptyContexts(MemoOperation operation, OperationRule rule)
    {
        Map<OperationRule, Set<RuleApplicationContext>> seenRules = seenNonEmptyContexts.get(operation);
        if (seenRules == null) {
            return null;
        }
        return seenRules.get(rule);
    }

    private void removeEmptyOperationEntryIfUnused(MemoOperation operation)
    {
        Set<OperationRule> seenRules = seenEmptyContexts.get(operation);
        if (seenRules != null && seenRules.isEmpty()) {
            seenEmptyContexts.remove(operation);
        }
    }
}
