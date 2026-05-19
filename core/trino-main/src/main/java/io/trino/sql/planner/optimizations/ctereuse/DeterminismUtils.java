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
package io.trino.sql.planner.optimizations.ctereuse;

import io.trino.sql.dialect.trino.operation.Call;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Operation;
import io.trino.sql.newir.Region;

import static io.trino.sql.dialect.trino.operationmetadata.CallOperationMetadata.RESOLVED_FUNCTION;

public class DeterminismUtils
{
    private DeterminismUtils() {}

    public static boolean isDeterministic(Block block)
    {
        return block.operations().stream()
                .allMatch(DeterminismUtils::isDeterministic);
    }

    public static boolean isDeterministic(Operation operation)
    {
        if (operation instanceof Call call && !RESOLVED_FUNCTION.getAttribute(call.attributes()).deterministic()) {
            return false;
        }

        return operation.regions().stream()
                .map(Region::getOnlyBlock)
                .allMatch(DeterminismUtils::isDeterministic);
    }
}
