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

import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.trino.spi.ErrorCode;
import io.trino.spi.TrinoException;

import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.spi.ErrorType.INSUFFICIENT_RESOURCES;
import static io.trino.spi.ErrorType.USER_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_GENERATION_ERROR;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE;
import static io.trino.spi.StandardErrorCode.REMOTE_SPLITS_TASK_QUEUE_FULL;

/**
 * Coordinator-side handling of remote splits task failures: rebuilding the worker's exception from
 * a failed HTTP response, and classifying which task-creation failures are worth retrying on
 * another worker.
 */
public final class RemoteSplitsTaskFailures
{
    private RemoteSplitsTaskFailures() {}

    public static TrinoException toException(JsonResponse<? extends RemoteSplitsTaskError> response, String defaultMessage)
    {
        RemoteSplitsTaskError error = null;
        if (response.hasValue()) {
            error = response.getValue();
        }
        if (error == null) {
            return new TrinoException(REMOTE_SPLITS_GENERATION_ERROR, defaultMessage);
        }
        String message = defaultMessage;
        if (error.errorMessage() != null) {
            message = error.errorMessage();
        }
        ErrorCode errorCode = error.errorCode();
        if (errorCode != null) {
            return new TrinoException(() -> errorCode, message);
        }
        return new TrinoException(REMOTE_SPLITS_GENERATION_ERROR, message);
    }

    /**
     * Task creation is retried on another worker only for failures plausibly specific to the chosen
     * worker: transport errors, HTTP 503, worker-internal errors, and the two conditions the worker
     * reports as its own transient shortages —
     * {@link io.trino.spi.StandardErrorCode#REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE} (its memory pool
     * is full right now) and {@link io.trino.spi.StandardErrorCode#REMOTE_SPLITS_TASK_QUEUE_FULL}
     * (its task-creation queue is saturated). Deterministic failures fail every node alike, so they
     * are not retried: user errors, external errors, and every other resource-limit error (per-node
     * and global memory limits are uniform across workers, cluster-wide shortages follow the query).
     */
    public static boolean isRetryableTaskCreationFailure(Exception failure)
    {
        if (!(failure instanceof TrinoException trinoException)) {
            // transport-level failure: the worker never produced a classified response
            return true;
        }
        ErrorCode errorCode = trinoException.getErrorCode();
        if (errorCode.getType() == USER_ERROR || errorCode.getType() == EXTERNAL) {
            return false;
        }
        if (errorCode.getType() == INSUFFICIENT_RESOURCES) {
            return errorCode.equals(REMOTE_SPLITS_TASK_MEMORY_UNAVAILABLE.toErrorCode())
                    || errorCode.equals(REMOTE_SPLITS_TASK_QUEUE_FULL.toErrorCode());
        }
        return true;
    }
}
