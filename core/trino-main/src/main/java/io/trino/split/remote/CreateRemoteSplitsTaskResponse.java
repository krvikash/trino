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
package io.trino.split.remote;

import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import jakarta.annotation.Nullable;

import static io.airlift.concurrent.MoreFutures.unwrapCompletionException;
import static java.util.Objects.requireNonNull;

/**
 * Response for the create-task endpoint: the connector's requested dynamic-filter wait timeout on
 * success, or an error message and code on failure. Carrying the original {@link TrinoException}
 * error code lets the coordinator decide whether creation is worth retrying on another worker —
 * deterministic failures (user errors, external errors, a reservation over the per-node memory
 * limit) fail every node alike. Kept separate from {@link RemoteSplitsTaskResponse} so the
 * timeout is not carried on every batch response.
 */
public record CreateRemoteSplitsTaskResponse(long requestedDynamicFilterWaitTimeoutMillis, @Nullable String errorMessage, @Nullable ErrorCode errorCode)
        implements RemoteSplitsTaskError
{
    public static CreateRemoteSplitsTaskResponse forCreatedTask(long requestedDynamicFilterWaitTimeoutMillis)
    {
        return new CreateRemoteSplitsTaskResponse(requestedDynamicFilterWaitTimeoutMillis, null, null);
    }

    public static CreateRemoteSplitsTaskResponse fail(Throwable throwable)
    {
        Throwable cause = unwrapCompletionException(requireNonNull(throwable, "throwable is null"));
        if (cause instanceof TrinoException trinoException) {
            return new CreateRemoteSplitsTaskResponse(0, trinoException.getMessage(), trinoException.getErrorCode());
        }
        return new CreateRemoteSplitsTaskResponse(0, cause.getMessage(), null);
    }
}
