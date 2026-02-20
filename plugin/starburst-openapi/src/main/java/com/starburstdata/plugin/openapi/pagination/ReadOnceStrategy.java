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

public class ReadOnceStrategy
        implements OpenApiPaginationStrategy<Boolean>
{
    @Override
    public Boolean initialState()
    {
        // Where boolean represents "have you read one response yet?"
        return false;
    }

    @Override
    public Boolean nextStateFromResponse(Boolean currentState, Response response)
    {
        return true;
    }

    @Override
    public Request nextRequestFromState(Request currentRequest, Boolean state)
    {
        return currentRequest;
    }

    @Override
    public boolean isFinished(Boolean state)
    {
        return state;
    }
}
