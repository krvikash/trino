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

import io.trino.sql.newir.Attributes;
import io.trino.sql.newir.DialectRegistry;
import io.trino.sql.planner.exploratory.MemoGroup;
import io.trino.sql.planner.exploratory.MemoOperation;

public interface Context
{
    MemoGroup getGroup(int groupId);

    /**
     * Returns the attributes derived for the equivalence group containing the operation.
     * This is the correct attribute view for rule matching and semantic decisions during
     * rule application.
     */
    Attributes getGroupAttributes(MemoOperation operation);

    DialectRegistry getDialectRegistry();
}
