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
package com.starburstdata.plugin.openapi.pagination;

import io.airlift.http.client.Request;
import io.airlift.http.client.Response;

/**
 * @param <S> The class representing the pagination "state".
 */
public interface OpenApiPaginationStrategy<S>
{
    OpenApiPaginationStrategy<?> READ_ONCE_STRATEGY = new ReadOnceStrategy();

    S initialState();

    S nextStateFromResponse(S currentState, Response response);

    Request nextRequestFromState(Request currentRequest, S state);

    boolean isFinished(S state);
}
