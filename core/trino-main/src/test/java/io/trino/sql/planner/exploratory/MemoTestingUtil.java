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

import com.google.common.collect.Sets;
import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.Operation.AttributeKey;

import java.util.Map;

public class MemoTestingUtil
{
    private MemoTestingUtil() {}

    public static void assertMemoGroup(MemoGroup actual, MemoGroupMatcher expected)
    {
        expected.match(actual);
    }

    public static void assertMemoGroups(Map<Integer, MemoGroup> actual, Map<Integer, MemoGroupMatcher> expected)
    {
        if (!expected.keySet().containsAll(actual.keySet())) {
            throw new AssertionError("Unexpected groups: " + Sets.difference(actual.keySet(), expected.keySet()));
        }
        if (!actual.keySet().containsAll(expected.keySet())) {
            throw new AssertionError("Missing expected groups: " + Sets.difference(expected.keySet(), actual.keySet()));
        }
        expected.forEach((groupId, matcher) -> matcher.match(actual.get(groupId)));
    }

    public static void assertMemoGroupsContains(Map<Integer, MemoGroup> actual, Map<Integer, MemoGroupMatcher> expected)
    {
        expected.forEach((groupId, matcher) -> {
            if (!actual.containsKey(groupId)) {
                throw new AssertionError("Missing expected group: " + groupId);
            }
            matcher.match(actual.get(groupId));
        });
    }

    public static void assertMemoGroupsDoesNotContain(Map<Integer, MemoGroup> actual, int... unexpected)
    {
        for (int groupId : unexpected) {
            if (actual.containsKey(groupId)) {
                throw new AssertionError("Unexpected group found: " + groupId);
            }
        }
    }

    public static Attributes attributes(Object... keyValues)
    {
        if (keyValues.length % 2 != 0) {
            throw new IllegalArgumentException("Expected alternating attribute keys and values");
        }

        Attributes.Builder builder = Attributes.builder();
        for (int i = 0; i < keyValues.length; i += 2) {
            builder.putUnchecked((AttributeKey) keyValues[i], keyValues[i + 1]);
        }
        return builder.buildOrThrow();
    }
}
