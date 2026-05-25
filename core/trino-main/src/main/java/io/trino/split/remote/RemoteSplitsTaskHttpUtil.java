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

import io.airlift.http.client.HttpUriBuilder;

import java.net.URI;

import static java.util.Objects.requireNonNull;

public final class RemoteSplitsTaskHttpUtil
{
    private RemoteSplitsTaskHttpUtil() {}

    public static URI remoteSplitsTaskLocation(URI internalUri)
    {
        requireNonNull(internalUri, "internalUri is null");
        return HttpUriBuilder.uriBuilderFrom(internalUri)
                .appendPath("/v1/splits/task")
                .build();
    }

    public static URI fetchRemoteSplitsTaskLocation(URI internalUri, String taskId)
    {
        requireNonNull(internalUri, "internalUri is null");
        requireNonNull(taskId, "taskId is null");
        return HttpUriBuilder.uriBuilderFrom(internalUri)
                .appendPath("/v1/splits/task")
                .appendPath(taskId)
                .appendPath("result")
                .build();
    }

    public static URI closeTaskLocation(URI internalUri, String taskId)
    {
        requireNonNull(internalUri, "internalUri is null");
        requireNonNull(taskId, "taskId is null");
        return HttpUriBuilder.uriBuilderFrom(internalUri)
                .appendPath("/v1/splits/task")
                .appendPath(taskId)
                .build();
    }

    public static URI heartbeatLocation(URI internalUri, String taskId)
    {
        requireNonNull(internalUri, "internalUri is null");
        requireNonNull(taskId, "taskId is null");
        return HttpUriBuilder.uriBuilderFrom(internalUri)
                .appendPath("/v1/splits/task")
                .appendPath(taskId)
                .appendPath("heartbeat")
                .build();
    }
}
