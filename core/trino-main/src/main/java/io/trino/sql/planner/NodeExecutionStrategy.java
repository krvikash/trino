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
package io.trino.sql.planner;

import com.google.inject.Inject;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.metadata.TableHandle;

import java.util.Collection;

public interface NodeExecutionStrategy
{
    NodeExecutionStrategy DISTRIBUTED_EXECUTION = (_, _) -> false;
    NodeExecutionStrategy SINGLE_NODE_EXECUTION = (_, _) -> true;

    boolean shouldBeExecutedOnSingleNode(Session session, Collection<TableHandle> tables);

    class DynamicNodeExecutionStrategy
            implements NodeExecutionStrategy
    {
        private final boolean preferSingleNodeExecution;
        private final Metadata metadata;

        @Inject
        public DynamicNodeExecutionStrategy(OptimizerConfig optimizerConfig, Metadata metadata)
        {
            this.preferSingleNodeExecution = optimizerConfig.isForceSingleNodeQuery();
            this.metadata = metadata;
        }

        @Override
        public boolean shouldBeExecutedOnSingleNode(Session session, Collection<TableHandle> tables)
        {
            return preferSingleNodeExecution
                    && tables.stream().allMatch(handle -> metadata.supportsSingleNodeExecution(session, handle));
        }
    }
}
