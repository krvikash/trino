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

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Composable transitive mapping that returns the most recent value for a given key.
 * When adding a new entry (a -> b), all existing entries that map to 'a' will effectively map to 'b'.
 * For any key that does not have a mapping, the identity mapping is returned.
 * <p>
 * Example:
 * Mapping mapping = new Mapping();
 * mapping.put(1, 2); // 1 -> 2
 * mapping.put(2, 3); // 1 -> 2 -> 3
 * mapping.getOrIdentity(1) // returns 3
 * mapping.getOrIdentity(42) // returns 42 (identity)
 * <p>
 * This implementation is not cycle-proof. The caller is responsible for avoiding cycles.
 */
public class Substitution<T>
{
    private final Map<T, T> mapping = new HashMap<>();

    public void put(T from, T to)
    {
        requireNonNull(from, "from is null");
        requireNonNull(to, "to is null");
        checkArgument(!mapping.containsKey(from), "Mapping for %s already exists", from);
        mapping.put(from, to);
    }

    public T getOrIdentity(T from)
    {
        requireNonNull(from, "from is null");
        if (!mapping.containsKey(from)) {
            return from;
        }

        // Find root with path compression
        T current = from;
        while (mapping.containsKey(current)) {
            current = mapping.get(current);
        }
        T root = current;

        // Path compression: update all nodes along the path to point directly to root
        current = from;
        while (mapping.containsKey(current)) {
            T next = mapping.get(current);
            if (!next.equals(root)) {
                mapping.put(current, root);
            }
            current = next;
        }

        return root;
    }
}
