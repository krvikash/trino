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
package io.trino.spi.security;

import io.trino.spi.connector.ConnectorSession;

import static java.util.Objects.requireNonNull;

public interface AiModelAccessControl
{
    AiModelAccessControl ALLOW_ALL = new AiModelAccessControl() {
        @Override
        public void checkCanExecuteModel(Context context, String modelId) {}
    };

    default void checkCanExecuteModel(Context context, String modelId)
    {
        denyExecuteAiModelAccess(modelId);
    }

    default void canCreateModel(Context context, String modelId)
    {
        denyCreateAiModelAccess(modelId);
    }

    default void canUpdateModel(Context context, String modelId)
    {
        denyUpdateAiModelAccess(modelId);
    }

    default void canDropModel(Context context, String modelId)
    {
        denyDropAiModelAccess(modelId);
    }

    default void denyExecuteAiModelAccess(String modelId)
    {
        denyAiModelAccess(modelId, "Execute");
    }

    default void denyCreateAiModelAccess(String modelId)
    {
        denyAiModelAccess(modelId, "Create");
    }

    default void denyUpdateAiModelAccess(String modelId)
    {
        denyAiModelAccess(modelId, "Update");
    }

    default void denyDropAiModelAccess(String modelId)
    {
        denyAiModelAccess(modelId, "Drop");
    }

    default void denyAiModelAccess(String modelId, String action)
    {
        throw new AccessDeniedException(action + " model " + modelId);
    }

    record Context(ConnectorIdentity connectorIdentity, String queryId)
    {
        public Context
        {
            requireNonNull(connectorIdentity, "connectorIdentity is null");
            requireNonNull(queryId, "queryId is null");
        }

        public Context(ConnectorSession session)
        {
            this(requireNonNull(session, "session is null").getIdentity(), session.getQueryId());
        }

        // overriding equals and hashCode to skip comparing connectorIdentity, because it is fixed for a given query
        @Override
        public boolean equals(Object other)
        {
            if (other == this) {
                return true;
            }
            if (other instanceof Context otherContext) {
                return queryId.equals(otherContext.queryId);
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return queryId.hashCode();
        }
    }
}
