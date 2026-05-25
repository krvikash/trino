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

import com.google.common.collect.ImmutableList;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.metrics.Metrics;
import jakarta.annotation.Nullable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.MoreFutures.unwrapCompletionException;
import static java.util.Objects.requireNonNull;

/**
 * Response envelope for a remote splits fetch: a batch of splits, whether the source is exhausted,
 * the source's metrics once it is, and an error message and code on failure. Carrying the original
 * {@link TrinoException} error code lets the coordinator rethrow with it instead of a generic
 * internal error, so a worker-side user error (e.g. permission denied) isn't misclassified. The
 * create-task response is separate ({@link CreateRemoteSplitsTaskResponse}).
 */
public record RemoteSplitsTaskResponse(
        long token,
        long nextToken,
        List<? extends ConnectorSplit> splits,
        boolean noMoreResults,
        @Nullable Metrics metrics,
        @Nullable String errorMessage,
        @Nullable ErrorCode errorCode)
        implements RemoteSplitsTaskError
{
    public RemoteSplitsTaskResponse
    {
        checkArgument(token >= 0, "token is negative");
        checkArgument(nextToken >= token, "nextToken is before token");
        splits = ImmutableList.copyOf(splits);
    }

    /**
     * The batch is not ready within the poll deadline; the coordinator should poll the same token again.
     */
    public static RemoteSplitsTaskResponse notReady(long token)
    {
        return new RemoteSplitsTaskResponse(token, token, ImmutableList.of(), false, null, null, null);
    }

    public static RemoteSplitsTaskResponse fail(long token, Throwable throwable)
    {
        Throwable cause = unwrapCompletionException(requireNonNull(throwable, "throwable is null"));
        ErrorCode errorCode = cause instanceof TrinoException trinoException ? trinoException.getErrorCode() : null;
        return new RemoteSplitsTaskResponse(token, token, ImmutableList.of(), true, null, cause.getMessage(), errorCode);
    }
}
