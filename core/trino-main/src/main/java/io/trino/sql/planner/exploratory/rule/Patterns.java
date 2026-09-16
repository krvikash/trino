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

import io.trino.matching.Pattern;
import io.trino.matching.Property;
import io.trino.sql.newir.Attributes;
import io.trino.sql.planner.exploratory.MemoOperation;
import io.trino.sql.planner.exploratory.MemoOperation.Child;

import java.util.List;

import static io.trino.matching.Property.property;

public class Patterns
{
    private Patterns() {}

    public static Pattern<MemoOperation> operation()
    {
        return Pattern.typeOf(MemoOperation.class);
    }

    /**
     * Matches the attributes derived for the matched operation's equivalence group.
     * Use this property when matching depends on derived semantic knowledge; {@link
     * MemoOperation#attributes()} exposes the stored operation attributes instead.
     */
    public static Property<MemoOperation, Context, Attributes> attributes()
    {
        return property("attributes", (memoOperation, context) -> context.getGroupAttributes(memoOperation));
    }

    public static Property<MemoOperation, Context, List<Child>> children()
    {
        return property("children", (memoOperation, _) -> memoOperation.children());
    }
}
