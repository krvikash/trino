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
package io.starburst.materialization.metastore.client;

import io.airlift.http.client.HttpStatusListener;
import io.starburst.materialization.metastore.client.RetryingHttpClient.RetryableException;

import static io.airlift.http.client.HttpStatus.BAD_GATEWAY;
import static io.airlift.http.client.HttpStatus.GATEWAY_TIMEOUT;
import static io.airlift.http.client.HttpStatus.SERVICE_UNAVAILABLE;

// Copied from io.starburst.stargate.http.InvalidServiceStatusListener.
// TODO: Replace it with the common class once available in cork
public class InvalidServiceStatusListener
        implements HttpStatusListener
{
    @Override
    public void statusReceived(int statusCode)
    {
        if ((statusCode == BAD_GATEWAY.code()) || (statusCode == GATEWAY_TIMEOUT.code()) || (statusCode == SERVICE_UNAVAILABLE.code())) {
            throw new RetryableException("Retryable status received: " + statusCode);
        }
    }
}
